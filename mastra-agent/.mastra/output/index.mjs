import { scoreTracesWorkflow } from '@mastra/core/evals/scoreTraces';
import { Mastra } from '@mastra/core/mastra';
import { LibSQLStore } from '@mastra/libsql';
import { Agent } from '@mastra/core/agent';
import { Memory } from '@mastra/memory';
import { createOpenAICompatible } from '@ai-sdk/openai-compatible';
import { a as analyzeClaimTool, l as listQueue, g as getQueue, b as analyzeClaim } from './overturn-tools.mjs';
import { createWorkflow, createStep } from '@mastra/core/workflows';
import { z } from 'zod';
import { existsSync, createReadStream, statSync } from 'fs';
import { readFile } from 'fs/promises';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';
import { Readable, Writable } from 'stream';
import * as https from 'https';
import { createServer, ServerResponse, STATUS_CODES } from 'http';
import { Http2ServerRequest, constants } from 'http2';
import process$1, { versions } from 'process';
import { Tool } from '@mastra/core/tools';
import { n as normalizeRoutePath, f as findMatchingCustomRoute, i as isReservedRequestContextKey, a as isStudioClientTypeHeader, M as MASTRA_IS_STUDIO_KEY, b as MASTRA_CLIENT_TYPE_HEADER, c as MASTRA_AUTH_MODE_KEY, d as coreAuthMiddleware, S as SERVER_ROUTES, p as pathMatchesPattern, e as isCustomRoutePublic, g as generateOpenAPIDocument, h as convertCustomRoutesToOpenAPIPaths, j as formatZodError, H as HTTPException$1, k as MASTRA_USER_KEY, l as MASTRA_AUTH_TOKEN_KEY, m as MASTRA_RESOURCE_ID_KEY, I as InMemoryTaskStore, o as isZodError } from './routes-CH2PZ0PI.mjs';
import { z as z$1 } from 'zod/v4';
import { RequestContext } from '@mastra/core/request-context';
import { MastraServerBase } from '@mastra/core/server';
import { safeStringify } from '@mastra/core/utils';
import { pipeline } from 'stream/promises';
import util from 'util';
import { Buffer as Buffer$1 } from 'buffer';
import { tools } from './tools.mjs';
import './rolldown-runtime-maW3aOIk.mjs';
import 'zod/v3';
import 'module';
import '@mastra/core/schema';
import '@mastra/core/utils/zod-to-json';
import '@mastra/core/auth/ee';
import '@mastra/core/memory';
import '@mastra/core/stream';
import '@mastra/core/agent/durable';
import '@mastra/core/di';
import '@mastra/core/error';
import '@mastra/core/llm';
import '@mastra/core/a2a';
import '@mastra/core/a2a/v1';
import 'dns/promises';
import 'net';
import '@mastra/core/storage';
import '@mastra/core/features';
import '@mastra/core/observability';
import '@mastra/core/evals';
import '@mastra/core/processors';
import '@mastra/core/workspace';

const ollama = createOpenAICompatible({
  name: "ollama",
  baseURL: process.env.OLLAMA_OPENAI_URL ?? "http://127.0.0.1:11434/v1"
});
const memory = new Memory({
  storage: new LibSQLStore({ id: "denial-desk-memory", url: "file:./denial-desk.db" }),
  options: { lastMessages: 20 }
});
const denialDesk = new Agent({
  name: "Denial Desk",
  instructions: [
    "You are the denial-management assistant for a small medical clinic's biller.",
    "Use list-denial-queue to see what is open and analyze-claim to work a specific claim.",
    "Always report: the denial reason in plain English, days left to appeal, the recommended move and why, and whether the letter passed the fact-check.",
    "Never invent claim numbers, amounts or codes: quote only what the tools return. If a tool fails, say so.",
    "A letter is a DRAFT for a human to review and sign. Never say an appeal will succeed; give the probability the tool returned."
  ].join("\n"),
  model: ollama(process.env.DENIAL_DESK_MODEL ?? "qwen2.5:7b-instruct"),
  tools: { listQueue, analyzeClaim: analyzeClaimTool },
  memory
});

const claimSummary = z.object({
  id: z.string(),
  payer: z.string(),
  denied: z.number(),
  best_move: z.string(),
  expected_value: z.number(),
  days_left: z.number()
});
const pickTop = createStep({
  id: "pick-top-claims",
  inputSchema: z.object({ top: z.number().int().min(1).max(10) }),
  outputSchema: z.object({ claims: z.array(claimSummary) }),
  execute: async ({ inputData }) => {
    const q = await getQueue();
    return {
      claims: q.items.filter((c) => c.best !== "writeoff").slice(0, inputData.top).map((c) => ({ id: c.id, payer: c.payer, denied: c.denied, best_move: c.best, expected_value: c.ev, days_left: c.days_left }))
    };
  }
});
const draftEach = createStep({
  id: "draft-each",
  inputSchema: z.object({ claims: z.array(claimSummary) }),
  outputSchema: z.object({
    drafts: z.array(z.object({ id: z.string(), move: z.string(), fact_check_passed: z.boolean(), engine: z.string() }))
  }),
  execute: async ({ inputData }) => {
    const drafts = [];
    for (const c of inputData.claims) {
      const r = await analyzeClaim(c.id);
      drafts.push({ id: c.id, move: r.triage.recommended, fact_check_passed: !!r.verify.passed, engine: r.letter.engine });
    }
    return { drafts };
  }
});
const report = createStep({
  id: "report",
  inputSchema: z.object({
    drafts: z.array(z.object({ id: z.string(), move: z.string(), fact_check_passed: z.boolean(), engine: z.string() }))
  }),
  outputSchema: z.object({ summary: z.string() }),
  execute: async ({ inputData }) => ({
    summary: inputData.drafts.map((d) => `${d.id}: ${d.move} (${d.fact_check_passed ? "fact-check passed" : "NEEDS REVIEW"}, ${d.engine})`).join("\n")
  })
});
const triageQueue = createWorkflow({
  id: "triage-queue",
  inputSchema: z.object({ top: z.number().int().min(1).max(10) }),
  outputSchema: z.object({ summary: z.string() })
}).then(pickTop).then(draftEach).then(report).commit();

const mastra = new Mastra({
  agents: { denialDesk },
  workflows: { triageQueue },
  storage: new LibSQLStore({ id: "overturn-mastra-store", url: "file:./mastra.db" })
});

/**
* Ensures that server.studioBase is normalized:
* - Adds leading slash if missing (e.g., 'admin' → '/admin')
* - Removes trailing slashes (e.g., '/admin/' → '/admin')
* - Normalizes multiple slashes to single slash (e.g., '//api' → '/api')
* - Returns empty string for root paths ('/' or '')
*
* @param studioBase - The studioBase path to normalize
* @returns Normalized studioBase path string
* @throws Error if path contains invalid characters ('..', '?', '#')
*/
function normalizeStudioBase(studioBase) {
	studioBase = studioBase.trim();
	if (studioBase.includes("..") || studioBase.includes("?") || studioBase.includes("#")) throw new Error(`Invalid base path: "${studioBase}". Base path cannot contain '..', '?', or '#'`);
	studioBase = studioBase.replace(/\/+/g, "/");
	if (studioBase === "/" || studioBase === "") return "";
	if (studioBase.endsWith("/")) studioBase = studioBase.slice(0, -1);
	if (!studioBase.startsWith("/")) studioBase = `/${studioBase}`;
	return studioBase;
}
/**
* Replace all `%%MASTRA_*%%` placeholders in the Studio `index.html` with the
* supplied configuration values.
*
* The `<base href>` tag and the `window.MASTRA_STUDIO_BASE_PATH` assignment
* use `basePath` as a plain string (no surrounding quotes), while all other
* placeholders replace `'%%TOKEN%%'` (with surrounding single-quotes in the
* source HTML) with the provided expression verbatim.
*/
function injectStudioHtmlConfig(html, config) {
	const replace = (token, value) => {
		html = html.replace(token, () => value);
	};
	replace(`'%%MASTRA_DEV_SERVER_INSTANCE_ID%%'`, config.devServerInstanceId ?? "''");
	replace(`'%%MASTRA_SERVER_HOST%%'`, config.host);
	replace(`'%%MASTRA_SERVER_PORT%%'`, config.port);
	replace(`'%%MASTRA_SERVER_PROTOCOL%%'`, config.protocol);
	replace(`'%%MASTRA_API_PREFIX%%'`, config.apiPrefix);
	replace(`'%%MASTRA_HIDE_CLOUD_CTA%%'`, config.hideCloudCta);
	replace(`'%%MASTRA_CLOUD_API_ENDPOINT%%'`, config.cloudApiEndpoint);
	replace(`'%%MASTRA_EXPERIMENTAL_FEATURES%%'`, config.experimentalFeatures);
	replace(`'%%MASTRA_TEMPLATES%%'`, config.templates);
	replace(`'%%MASTRA_TELEMETRY_DISABLED%%'`, config.telemetryDisabled);
	replace(`'%%MASTRA_REQUEST_CONTEXT_PRESETS%%'`, config.requestContextPresets);
	replace(`'%%MASTRA_EXPERIMENTAL_UI%%'`, config.experimentalUI);
	replace(`'%%MASTRA_AGENT_SIGNALS%%'`, config.agentSignals);
	replace(`'%%MASTRA_SIGNALS_UI%%'`, config.signalsUI);
	replace(`'%%MASTRA_ORGANIZATION_ID%%'`, config.organizationId);
	replace(`'%%MASTRA_PLATFORM_PROJECT_ID%%'`, config.platformProjectId);
	replace(`'%%MASTRA_PLATFORM_OBSERVABILITY_ENDPOINT%%'`, config.platformObservabilityEndpoint);
	if (config.autoDetectUrl) replace(`'%%MASTRA_AUTO_DETECT_URL%%'`, config.autoDetectUrl);
	html = html.replaceAll("%%MASTRA_STUDIO_BASE_PATH%%", () => config.basePath);
	return html;
}
/**
* Escape a dynamic value for embedding inside a single-quoted JavaScript
* string literal in the Studio `index.html` (e.g. `window.X = '<value>'`).
* Without it, an env-derived value containing `'` or `<\/script>` breaks out
* of the literal and corrupts (or injects into) the served page.
*/
function escapeStudioHtmlValue(value) {
	return value.replace(/\\/g, "\\\\").replace(/'/g, "\\'").replace(/\n/g, "\\n").replace(/\r/g, "\\r").replace(/</g, "\\u003c").replace(/>/g, "\\u003e").replace(/\u2028/g, "\\u2028").replace(/\u2029/g, "\\u2029");
}

//#region src/utils/mime.ts
/**
* @module
* MIME utility.
*/
const getMimeType = (filename, mimes = baseMimes) => {
	const match = filename.match(/\.([a-zA-Z0-9]+?)$/);
	if (!match) return;
	return mimes[match[1].toLowerCase()];
};
const baseMimes = {
	aac: "audio/aac",
	avi: "video/x-msvideo",
	avif: "image/avif",
	av1: "video/av1",
	bin: "application/octet-stream",
	bmp: "image/bmp",
	css: "text/css; charset=utf-8",
	csv: "text/csv; charset=utf-8",
	eot: "application/vnd.ms-fontobject",
	epub: "application/epub+zip",
	gif: "image/gif",
	gz: "application/gzip",
	htm: "text/html; charset=utf-8",
	html: "text/html; charset=utf-8",
	ico: "image/x-icon",
	ics: "text/calendar; charset=utf-8",
	jpeg: "image/jpeg",
	jpg: "image/jpeg",
	js: "text/javascript; charset=utf-8",
	json: "application/json",
	jsonld: "application/ld+json",
	map: "application/json",
	mid: "audio/x-midi",
	midi: "audio/x-midi",
	mjs: "text/javascript; charset=utf-8",
	mp3: "audio/mpeg",
	mp4: "video/mp4",
	mpeg: "video/mpeg",
	oga: "audio/ogg",
	ogv: "video/ogg",
	ogx: "application/ogg",
	opus: "audio/opus",
	otf: "font/otf",
	pdf: "application/pdf",
	png: "image/png",
	rtf: "application/rtf",
	svg: "image/svg+xml; charset=utf-8",
	tif: "image/tiff",
	tiff: "image/tiff",
	ts: "video/mp2t",
	ttf: "font/ttf",
	txt: "text/plain; charset=utf-8",
	wasm: "application/wasm",
	webm: "video/webm",
	weba: "audio/webm",
	webmanifest: "application/manifest+json",
	webp: "image/webp",
	woff: "font/woff",
	woff2: "font/woff2",
	xhtml: "application/xhtml+xml; charset=utf-8",
	xml: "application/xml; charset=utf-8",
	zip: "application/zip",
	"3gp": "video/3gpp",
	"3g2": "video/3gpp2",
	gltf: "model/gltf+json",
	glb: "model/gltf-binary"
};

//#region src/utils/html.ts
/**
* @module
* HTML utility.
*/
const HtmlEscapedCallbackPhase = {
	Stringify: 1};
const raw = (value, callbacks) => {
	const escapedString = new String(value);
	escapedString.isEscaped = true;
	escapedString.callbacks = callbacks;
	return escapedString;
};
const escapeRe = /[&<>'"]/;
const stringBufferToString = async (buffer, callbacks) => {
	let str = "";
	callbacks ||= [];
	const resolvedBuffer = await Promise.all(buffer);
	for (let i = resolvedBuffer.length - 1;; i--) {
		str += resolvedBuffer[i];
		i--;
		if (i < 0) break;
		let r = resolvedBuffer[i];
		if (typeof r === "object") callbacks.push(...r.callbacks || []);
		const isEscaped = r.isEscaped;
		r = await (typeof r === "object" ? r.toString() : r);
		if (typeof r === "object") callbacks.push(...r.callbacks || []);
		if (r.isEscaped ?? isEscaped) str += r;
		else {
			const buf = [str];
			escapeToBuffer(r, buf);
			str = buf[0];
		}
	}
	return raw(str, callbacks);
};
const escapeToBuffer = (str, buffer) => {
	const match = str.search(escapeRe);
	if (match === -1) {
		buffer[0] += str;
		return;
	}
	let escape;
	let index;
	let lastIndex = 0;
	for (index = match; index < str.length; index++) {
		switch (str.charCodeAt(index)) {
			case 34:
				escape = "&quot;";
				break;
			case 39:
				escape = "&#39;";
				break;
			case 38:
				escape = "&amp;";
				break;
			case 60:
				escape = "&lt;";
				break;
			case 62:
				escape = "&gt;";
				break;
			default: continue;
		}
		buffer[0] += str.substring(lastIndex, index) + escape;
		lastIndex = index + 1;
	}
	buffer[0] += str.substring(lastIndex, index);
};
const resolveCallbackSync = (str) => {
	const callbacks = str.callbacks;
	if (!callbacks?.length) return str;
	const buffer = [str];
	const context = {};
	callbacks.forEach((c) => c({
		phase: HtmlEscapedCallbackPhase.Stringify,
		buffer,
		context
	}));
	return buffer[0];
};
const resolveCallback = async (str, phase, preserveCallbacks, context, buffer) => {
	if (typeof str === "object" && !(str instanceof String)) {
		if (!(str instanceof Promise)) str = str.toString();
		if (str instanceof Promise) str = await str;
	}
	const callbacks = str.callbacks;
	if (!callbacks?.length) return Promise.resolve(str);
	if (buffer) buffer[0] += str;
	else buffer = [str];
	const resStr = Promise.all(callbacks.map((c) => c({
		phase,
		buffer,
		context
	}))).then((res) => Promise.all(res.filter(Boolean).map((str) => resolveCallback(str, phase, false, context, buffer))).then(() => buffer[0]));
	return resStr;
};

//#region src/helper/html/index.ts
/**
* @module
* html Helper for Hono.
*/
const html = (strings, ...values) => {
	const buffer = [""];
	for (let i = 0, len = strings.length - 1; i < len; i++) {
		buffer[0] += strings[i];
		const children = Array.isArray(values[i]) ? values[i].flat(Infinity) : [values[i]];
		for (let i = 0, len = children.length; i < len; i++) {
			const child = children[i];
			if (typeof child === "string") escapeToBuffer(child, buffer);
			else if (typeof child === "number") buffer[0] += child;
			else if (typeof child === "boolean" || child === null || child === void 0) continue;
			else if (typeof child === "object" && child.isEscaped) {
				if (child.callbacks) buffer.unshift("", child);
				else {
					const tmp = child.toString();
					if (tmp instanceof Promise) buffer.unshift("", tmp);
					else buffer[0] += tmp;
				}
			} else if (child instanceof Promise) buffer.unshift("", child);
			else escapeToBuffer(child.toString(), buffer);
		}
	}
	buffer[0] += strings.at(-1);
	return buffer.length === 1 ? "callbacks" in buffer ? raw(resolveCallbackSync(raw(buffer[0], buffer.callbacks))) : raw(buffer[0]) : stringBufferToString(buffer, buffer.callbacks);
};

//#region src/http-exception.ts
/**
* `HTTPException` must be used when a fatal error such as authentication failure occurs.
*
* @see {@link https://hono.dev/docs/api/exception}
*
* @param {StatusCode} status - status code of HTTPException
* @param {HTTPExceptionOptions} options - options of HTTPException
* @param {HTTPExceptionOptions["res"]} options.res - response of options of HTTPException
* @param {HTTPExceptionOptions["message"]} options.message - message of options of HTTPException
* @param {HTTPExceptionOptions["cause"]} options.cause - cause of options of HTTPException
*
* @example
* ```ts
* import { HTTPException } from 'hono/http-exception'
*
* // ...
*
* app.post('/auth', async (c, next) => {
*   // authentication
*   if (authorized === false) {
*     throw new HTTPException(401, { message: 'Custom error message' })
*   }
*   await next()
* })
* ```
*/
var HTTPException = class extends Error {
	res;
	status;
	/**
	* Creates an instance of `HTTPException`.
	* @param status - HTTP status code for the exception. Defaults to 500.
	* @param options - Additional options for the exception.
	*/
	constructor(status = 500, options) {
		super(options?.message, { cause: options?.cause });
		this.res = options?.res;
		this.status = status;
	}
	/**
	* Returns the response object associated with the exception.
	* If a response object is not provided, a new response is created with the error message and status code.
	* @returns The response object.
	*/
	getResponse() {
		if (this.res) return new Response(this.res.body, {
			status: this.status,
			headers: this.res.headers
		});
		return new Response(this.message, { status: this.status });
	}
};

//#region src/request/constants.ts
const GET_MATCH_RESULT = Symbol();

const bufferToFormData = (arrayBuffer, contentType) => {
	return new Response(arrayBuffer, { headers: { "Content-Type": contentType.replace(/^[^;]+/, (mediaType) => mediaType.toLowerCase()) } }).formData();
};

//#region src/utils/body.ts
const MAX_NESTED_OBJECTS = 1e4;
const isRawRequest = (request) => "headers" in request;
const parseBody = async (request, options = Object.create(null)) => {
	const { all = false, dot = false } = options;
	const mediaType = (isRawRequest(request) ? request.headers : request.raw.headers).get("Content-Type")?.split(";")[0].trim().toLowerCase();
	if (mediaType === "multipart/form-data" || mediaType === "application/x-www-form-urlencoded") return parseFormData(request, {
		all,
		dot
	});
	return {};
};
/**
* Parses form data from a request.
*
* @template T - The type of the parsed body data.
* @param {HonoRequest | Request} request - The request object containing form data.
* @param {ParseBodyOptions} options - Options for parsing the form data.
* @returns {Promise<T>} The parsed body data.
*/
async function parseFormData(request, options) {
	if (!isRawRequest(request) && request.bodyCache.formData) return convertFormDataToBodyData(await request.bodyCache.formData, options);
	const headers = isRawRequest(request) ? request.headers : request.raw.headers;
	const arrayBuffer = await request.arrayBuffer();
	const formDataPromise = bufferToFormData(arrayBuffer, headers.get("Content-Type") || "");
	if (!isRawRequest(request)) request.bodyCache.formData = formDataPromise;
	const formData = await formDataPromise;
	if (formData) return convertFormDataToBodyData(formData, options);
	return {};
}
/**
* Converts form data to body data based on the provided options.
*
* @template T - The type of the parsed body data.
* @param {FormData} formData - The form data to convert.
* @param {ParseBodyOptions} options - Options for parsing the form data.
* @returns {T} The converted body data.
*/
function convertFormDataToBodyData(formData, options) {
	const form = Object.create(null);
	const nestingState = { count: 0 };
	formData.forEach((value, key) => {
		if (!(options.all || key.endsWith("[]"))) form[key] = value;
		else handleParsingAllValues(form, key, value);
	});
	if (options.dot) Object.entries(form).forEach(([key, value]) => {
		if (key.includes(".")) {
			handleParsingNestedValues(form, key, value, nestingState);
			delete form[key];
		}
	});
	return form;
}
/**
* Handles parsing all values for a given key, supporting multiple values as arrays.
*
* @param {BodyData} form - The form data object.
* @param {string} key - The key to parse.
* @param {FormDataEntryValue} value - The value to assign.
*/
const handleParsingAllValues = (form, key, value) => {
	if (form[key] !== void 0) {
		if (Array.isArray(form[key])) form[key].push(value);
		else form[key] = [form[key], value];
	} else if (!key.endsWith("[]")) form[key] = value;
	else form[key] = [value];
};
/**
* Handles parsing nested values using dot notation keys.
*
* @param {BodyData} form - The form data object.
* @param {string} key - The dot notation key.
* @param {BodyDataValue} value - The value to assign.
*/
const handleParsingNestedValues = (form, key, value, state) => {
	if (/(?:^|\.)__proto__\./.test(key)) return;
	let nestedForm = form;
	const keys = key.split(".", 34);
	if (keys.length > 33) throwNestingLimitExceeded();
	keys.forEach((key, index) => {
		if (index === keys.length - 1) nestedForm[key] = value;
		else {
			if (!nestedForm[key] || typeof nestedForm[key] !== "object" || Array.isArray(nestedForm[key]) || nestedForm[key] instanceof File) {
				if (state.count++ >= MAX_NESTED_OBJECTS) throwNestingLimitExceeded();
				nestedForm[key] = Object.create(null);
			}
			nestedForm = nestedForm[key];
		}
	});
};
const throwNestingLimitExceeded = () => {
	throw new Error("Nesting limit exceeded");
};

//#region src/utils/url.ts
const splitPath = (path) => {
	const paths = path.split("/");
	if (paths[0] === "") paths.shift();
	return paths;
};
const splitRoutingPath = (routePath) => {
	const { groups, path } = extractGroupsFromPath(routePath);
	const paths = splitPath(path);
	return replaceGroupMarks(paths, groups);
};
const extractGroupsFromPath = (path) => {
	const groups = [];
	path = path.replace(/\{[^}]+\}/g, (match, index) => {
		const mark = `@${index}`;
		groups.push([mark, match]);
		return mark;
	});
	return {
		groups,
		path
	};
};
const replaceGroupMarks = (paths, groups) => {
	for (let i = groups.length - 1; i >= 0; i--) {
		const [mark] = groups[i];
		for (let j = paths.length - 1; j >= 0; j--) if (paths[j].includes(mark)) {
			paths[j] = paths[j].replace(mark, groups[i][1]);
			break;
		}
	}
	return paths;
};
const patternCache = {};
const getPattern = (label, next) => {
	if (label === "*") return "*";
	const match = label.match(/^\:([^\{\}]+)(?:\{(.+)\})?$/);
	if (match) {
		const cacheKey = `${label}#${next}`;
		if (!patternCache[cacheKey]) {
			if (match[2]) patternCache[cacheKey] = next && next[0] !== ":" && next[0] !== "*" ? [
				cacheKey,
				match[1],
				new RegExp(`^${match[2]}(?=/${next})`)
			] : [
				label,
				match[1],
				new RegExp(`^${match[2]}$`)
			];
			else patternCache[cacheKey] = [
				label,
				match[1],
				true
			];
		}
		return patternCache[cacheKey];
	}
	return null;
};
const tryDecode$1 = (str, decoder) => {
	try {
		return decoder(str);
	} catch {
		return str.replace(/(?:%[0-9A-Fa-f]{2})+/g, (match) => {
			try {
				return decoder(match);
			} catch {
				return match;
			}
		});
	}
};
/**
* Try to apply decodeURI() to given string.
* If it fails, skip invalid percent encoding or invalid UTF-8 sequences, and apply decodeURI() to the rest as much as possible.
* @param str The string to decode.
* @returns The decoded string that sometimes contains undecodable percent encoding.
* @example
* tryDecodeURI('Hello%20World') // 'Hello World'
* tryDecodeURI('Hello%20World/%A4%A2') // 'Hello World/%A4%A2'
*/
const tryDecodeURI$1 = (str) => tryDecode$1(str, decodeURI);
const getPath = (request) => {
	const url = request.url;
	const start = url.indexOf("/", url.indexOf(":") + 4);
	let i = start;
	for (; i < url.length; i++) {
		const charCode = url.charCodeAt(i);
		if (charCode === 37) {
			const queryIndex = url.indexOf("?", i);
			const hashIndex = url.indexOf("#", i);
			const end = queryIndex === -1 ? hashIndex === -1 ? void 0 : hashIndex : hashIndex === -1 ? queryIndex : Math.min(queryIndex, hashIndex);
			const path = url.slice(start, end);
			return tryDecodeURI$1(path.includes("%25") ? path.replace(/%25/g, "%2525") : path);
		} else if (charCode === 63 || charCode === 35) break;
	}
	return url.slice(start, i);
};
const getPathNoStrict = (request) => {
	const result = getPath(request);
	return result.length > 1 && result.at(-1) === "/" ? result.slice(0, -1) : result;
};
/**
* Merge paths.
* @param {string[]} ...paths - The paths to merge.
* @returns {string} The merged path.
* @example
* mergePath('/api', '/users') // '/api/users'
* mergePath('/api/', '/users') // '/api/users'
* mergePath('/api', '/') // '/api'
* mergePath('/api/', '/') // '/api/'
*/
const mergePath = (base, sub, ...rest) => {
	if (rest.length) sub = mergePath(sub, ...rest);
	return `${base?.[0] === "/" ? "" : "/"}${base}${sub === "/" ? "" : `${base?.at(-1) === "/" ? "" : "/"}${sub?.[0] === "/" ? sub.slice(1) : sub}`}`;
};
const checkOptionalParameter = (path) => {
	if (path.charCodeAt(path.length - 1) !== 63 || !path.includes(":")) return null;
	const segments = path.split("/");
	const results = [];
	let basePath = "";
	segments.forEach((segment) => {
		if (segment !== "" && !/\:/.test(segment)) basePath += "/" + segment;
		else if (/\:/.test(segment)) {
			if (segment.charCodeAt(segment.length - 1) === 63) {
				if (results.length === 0 && basePath === "") results.push("/");
				else results.push(basePath);
				const optionalSegment = segment.slice(0, -1);
				basePath += "/" + optionalSegment;
				results.push(basePath);
			} else basePath += "/" + segment;
		}
	});
	return results.filter((v, i, a) => a.indexOf(v) === i);
};
const tryDecodeURIComponent = (str) => str.indexOf("%") !== -1 ? tryDecode$1(str, decodeURIComponent_) : str;
const _decodeURI = (value) => {
	if (value.indexOf("+") !== -1) value = value.replace(/\+/g, " ");
	return tryDecodeURIComponent(value);
};
const _getQueryParam = (url, key, multiple) => {
	const hashIndex = url.indexOf("#", 8);
	if (hashIndex !== -1) url = url.slice(0, hashIndex);
	let encoded;
	if (!multiple && key && key.indexOf("%") === -1 && key.indexOf("+") === -1) {
		let keyIndex = url.indexOf("?", 8);
		if (keyIndex === -1) return;
		if (!url.startsWith(key, keyIndex + 1)) keyIndex = url.indexOf(`&${key}`, keyIndex + 1);
		while (keyIndex !== -1) {
			const trailingKeyCode = url.charCodeAt(keyIndex + key.length + 1);
			if (trailingKeyCode === 61) {
				const valueIndex = keyIndex + key.length + 2;
				const endIndex = url.indexOf("&", valueIndex);
				return _decodeURI(url.slice(valueIndex, endIndex === -1 ? void 0 : endIndex));
			} else if (trailingKeyCode == 38 || isNaN(trailingKeyCode)) return "";
			keyIndex = url.indexOf(`&${key}`, keyIndex + 1);
		}
		encoded = /[%+]/.test(url);
		if (!encoded) return;
	}
	const results = Object.create(null);
	encoded ??= /[%+]/.test(url);
	let keyIndex = url.indexOf("?", 8);
	while (keyIndex !== -1) {
		const nextKeyIndex = url.indexOf("&", keyIndex + 1);
		let valueIndex = url.indexOf("=", keyIndex);
		if (valueIndex > nextKeyIndex && nextKeyIndex !== -1) valueIndex = -1;
		let name = url.slice(keyIndex + 1, valueIndex === -1 ? nextKeyIndex === -1 ? void 0 : nextKeyIndex : valueIndex);
		if (encoded) name = _decodeURI(name);
		keyIndex = nextKeyIndex;
		if (name === "") continue;
		let value;
		if (valueIndex === -1) value = "";
		else {
			value = url.slice(valueIndex + 1, nextKeyIndex === -1 ? void 0 : nextKeyIndex);
			if (encoded) value = _decodeURI(value);
		}
		if (multiple) {
			if (!(results[name] && Array.isArray(results[name]))) results[name] = [];
			results[name].push(value);
		} else results[name] ??= value;
	}
	return key ? results[key] : results;
};
const getQueryParam = _getQueryParam;
const getQueryParams = (url, key) => {
	return _getQueryParam(url, key, true);
};
const decodeURIComponent_ = decodeURIComponent;

//#region src/request.ts
var HonoRequest = class {
	/**
	* `.raw` can get the raw Request object.
	*
	* @see {@link https://hono.dev/docs/api/request#raw}
	*
	* @example
	* ```ts
	* // For Cloudflare Workers
	* app.post('/', async (c) => {
	*   const metadata = c.req.raw.cf?.hostMetadata?
	*   ...
	* })
	* ```
	*/
	raw;
	#validatedData;
	#matchResult;
	routeIndex = 0;
	/**
	* `.path` can get the pathname of the request.
	*
	* @see {@link https://hono.dev/docs/api/request#path}
	*
	* @example
	* ```ts
	* app.get('/about/me', (c) => {
	*   const pathname = c.req.path // `/about/me`
	* })
	* ```
	*/
	path;
	bodyCache = {};
	constructor(request, path = "/", matchResult = [[]]) {
		this.raw = request;
		this.path = path;
		this.#matchResult = matchResult;
	}
	param(key) {
		return key ? this.#getDecodedParam(key) : this.#getAllDecodedParams();
	}
	#getDecodedParam(key) {
		const paramKey = this.#matchResult[0][this.routeIndex]?.[1][key];
		const param = this.#getParamValue(paramKey);
		return param && tryDecodeURIComponent(param);
	}
	#getAllDecodedParams() {
		const decoded = {};
		const keys = Object.keys(this.#matchResult[0][this.routeIndex]?.[1] ?? {});
		for (const key of keys) {
			const value = this.#getParamValue(this.#matchResult[0][this.routeIndex][1][key]);
			if (value !== void 0) decoded[key] = tryDecodeURIComponent(value);
		}
		return decoded;
	}
	#getParamValue(paramKey) {
		return this.#matchResult[1] ? this.#matchResult[1][paramKey] : paramKey;
	}
	query(key) {
		return getQueryParam(this.url, key);
	}
	queries(key) {
		return getQueryParams(this.url, key);
	}
	header(name) {
		if (name) return this.raw.headers.get(name) ?? void 0;
		const headerData = Object.create(null);
		this.raw.headers.forEach((value, key) => {
			headerData[key] = value;
		});
		return headerData;
	}
	async parseBody(options) {
		return parseBody(this, options);
	}
	#cachedBody = (key) => {
		const { bodyCache, raw } = this;
		const cachedBody = bodyCache[key];
		if (cachedBody) return cachedBody;
		for (const anyCachedKey in bodyCache) return bodyCache[anyCachedKey].then((body) => {
			if (anyCachedKey === "json") body = JSON.stringify(body);
			const contentType = anyCachedKey === "formData" ? void 0 : raw.headers.get("content-type");
			return new Response(body, { headers: contentType ? { "Content-Type": contentType } : void 0 })[key]();
		});
		return bodyCache[key] = raw[key]();
	};
	/**
	* `.json()` can parse Request body of type `application/json`
	*
	* @see {@link https://hono.dev/docs/api/request#json}
	*
	* @example
	* ```ts
	* app.post('/entry', async (c) => {
	*   const body = await c.req.json()
	* })
	* ```
	*/
	json() {
		return this.#cachedBody("text").then((text) => JSON.parse(text));
	}
	/**
	* `.text()` can parse Request body of type `text/plain`
	*
	* @see {@link https://hono.dev/docs/api/request#text}
	*
	* @example
	* ```ts
	* app.post('/entry', async (c) => {
	*   const body = await c.req.text()
	* })
	* ```
	*/
	text() {
		return this.#cachedBody("text");
	}
	/**
	* `.arrayBuffer()` parse Request body as an `ArrayBuffer`
	*
	* @see {@link https://hono.dev/docs/api/request#arraybuffer}
	*
	* @example
	* ```ts
	* app.post('/entry', async (c) => {
	*   const body = await c.req.arrayBuffer()
	* })
	* ```
	*/
	arrayBuffer() {
		return this.#cachedBody("arrayBuffer");
	}
	/**
	* `.bytes()` parses the request body as a `Uint8Array`.
	*
	* @see {@link https://hono.dev/docs/api/request#bytes}
	*
	* @example
	* ```ts
	* app.post('/entry', async (c) => {
	*   const body = await c.req.bytes()
	* })
	* ```
	*/
	bytes() {
		return this.#cachedBody("arrayBuffer").then((buffer) => new Uint8Array(buffer));
	}
	/**
	* Parses the request body as a `Blob`.
	* @example
	* ```ts
	* app.post('/entry', async (c) => {
	*   const body = await c.req.blob();
	* });
	* ```
	* @see https://hono.dev/docs/api/request#blob
	*/
	blob() {
		return this.#cachedBody("blob");
	}
	/**
	* Parses the request body as `FormData`.
	* @example
	* ```ts
	* app.post('/entry', async (c) => {
	*   const body = await c.req.formData();
	* });
	* ```
	* @see https://hono.dev/docs/api/request#formdata
	*/
	formData() {
		return this.#cachedBody("formData");
	}
	/**
	* Adds validated data to the request.
	*
	* @param target - The target of the validation.
	* @param data - The validated data to add.
	*/
	addValidatedData(target, data) {
		(this.#validatedData ??= {})[target] = data;
	}
	valid(target) {
		return this.#validatedData?.[target];
	}
	/**
	* `.url` can get the request url strings.
	*
	* @see {@link https://hono.dev/docs/api/request#url}
	*
	* @example
	* ```ts
	* app.get('/about/me', (c) => {
	*   const url = c.req.url // `http://localhost:8787/about/me`
	*   ...
	* })
	* ```
	*/
	get url() {
		return this.raw.url;
	}
	/**
	* `.method` can get the method name of the request.
	*
	* @see {@link https://hono.dev/docs/api/request#method}
	*
	* @example
	* ```ts
	* app.get('/about/me', (c) => {
	*   const method = c.req.method // `GET`
	* })
	* ```
	*/
	get method() {
		return this.raw.method;
	}
	get [GET_MATCH_RESULT]() {
		return this.#matchResult;
	}
	/**
	* `.matchedRoutes` can return a matched route in the handler
	*
	* @deprecated
	*
	* Use matchedRoutes helper defined in "hono/route" instead.
	*
	* @see {@link https://hono.dev/docs/api/request#matchedroutes}
	*
	* @example
	* ```ts
	* app.use('*', async function logger(c, next) {
	*   await next()
	*   c.req.matchedRoutes.forEach(({ handler, method, path }, i) => {
	*     const name = handler.name || (handler.length < 2 ? '[handler]' : '[middleware]')
	*     console.log(
	*       method,
	*       ' ',
	*       path,
	*       ' '.repeat(Math.max(10 - path.length, 0)),
	*       name,
	*       i === c.req.routeIndex ? '<- respond from here' : ''
	*     )
	*   })
	* })
	* ```
	*/
	get matchedRoutes() {
		return this.#matchResult[0].map(([[, route]]) => route);
	}
	/**
	* `.routePath` can retrieve the path registered within the handler
	*
	* @deprecated
	*
	* Use routePath helper defined in "hono/route" instead.
	*
	* @see {@link https://hono.dev/docs/api/request#routepath}
	*
	* @example
	* ```ts
	* app.get('/posts/:id', (c) => {
	*   return c.json({ path: c.req.routePath })
	* })
	* ```
	*/
	get routePath() {
		return this.#matchResult[0].map(([[, route]]) => route)[this.routeIndex].path;
	}
};

//#region src/context.ts
const TEXT_PLAIN = "text/plain; charset=UTF-8";
const setDefaultContentType = (contentType, headers) => {
	return {
		"Content-Type": contentType,
		...headers
	};
};
const createResponseInstance = (body, init) => new Response(body, init);
var Context = class {
	#rawRequest;
	#req;
	/**
	* `.env` can get bindings (environment variables, secrets, KV namespaces, D1 database, R2 bucket etc.) in Cloudflare Workers.
	*
	* @see {@link https://hono.dev/docs/api/context#env}
	*
	* @example
	* ```ts
	* // Environment object for Cloudflare Workers
	* app.get('*', async c => {
	*   const counter = c.env.COUNTER
	* })
	* ```
	*/
	env = {};
	#var;
	finalized = false;
	/**
	* `.error` can get the error object from the middleware if the Handler throws an error.
	*
	* @see {@link https://hono.dev/docs/api/context#error}
	*
	* @example
	* ```ts
	* app.use('*', async (c, next) => {
	*   await next()
	*   if (c.error) {
	*     // do something...
	*   }
	* })
	* ```
	*/
	error;
	#status;
	#executionCtx;
	#res;
	#layout;
	#renderer;
	#notFoundHandler;
	#preparedHeaders;
	#matchResult;
	#path;
	/**
	* Creates an instance of the Context class.
	*
	* @param req - The Request object.
	* @param options - Optional configuration options for the context.
	*/
	constructor(req, options) {
		this.#rawRequest = req;
		if (options) {
			this.#executionCtx = options.executionCtx;
			this.env = options.env;
			this.#notFoundHandler = options.notFoundHandler;
			this.#path = options.path;
			this.#matchResult = options.matchResult;
		}
	}
	/**
	* `.req` is the instance of {@link HonoRequest}.
	*/
	get req() {
		this.#req ??= new HonoRequest(this.#rawRequest, this.#path, this.#matchResult);
		return this.#req;
	}
	/**
	* @see {@link https://hono.dev/docs/api/context#event}
	* The FetchEvent associated with the current request.
	*
	* @throws Will throw an error if the context does not have a FetchEvent.
	*/
	get event() {
		if (this.#executionCtx && "respondWith" in this.#executionCtx) return this.#executionCtx;
		else throw Error("This context has no FetchEvent");
	}
	/**
	* @see {@link https://hono.dev/docs/api/context#executionctx}
	* The ExecutionContext associated with the current request.
	*
	* @throws Will throw an error if the context does not have an ExecutionContext.
	*/
	get executionCtx() {
		if (this.#executionCtx) return this.#executionCtx;
		else throw Error("This context has no ExecutionContext");
	}
	/**
	* @see {@link https://hono.dev/docs/api/context#res}
	* The Response object for the current request.
	*/
	get res() {
		return this.#res ||= createResponseInstance(null, { headers: this.#preparedHeaders ??= new Headers() });
	}
	/**
	* Sets the Response object for the current request.
	*
	* @param _res - The Response object to set.
	*/
	set res(_res) {
		if (this.#res && _res) {
			_res = createResponseInstance(_res.body, _res);
			for (const [k, v] of this.#res.headers.entries()) {
				if (k === "content-type") continue;
				if (k === "set-cookie") {
					const cookies = this.#res.headers.getSetCookie();
					_res.headers.delete("set-cookie");
					for (const cookie of cookies) _res.headers.append("set-cookie", cookie);
				} else _res.headers.set(k, v);
			}
		}
		this.#res = _res;
		this.finalized = true;
	}
	/**
	* `.render()` can create a response within a layout.
	*
	* @see {@link https://hono.dev/docs/api/context#render-setrenderer}
	*
	* @example
	* ```ts
	* app.get('/', (c) => {
	*   return c.render('Hello!')
	* })
	* ```
	*/
	render = (...args) => {
		this.#renderer ??= (content) => this.html(content);
		return this.#renderer(...args);
	};
	/**
	* Sets the layout for the response.
	*
	* @param layout - The layout to set.
	* @returns The layout function.
	*/
	setLayout = (layout) => this.#layout = layout;
	/**
	* Gets the current layout for the response.
	*
	* @returns The current layout function.
	*/
	getLayout = () => this.#layout;
	/**
	* `.setRenderer()` can set the layout in the custom middleware.
	*
	* @see {@link https://hono.dev/docs/api/context#render-setrenderer}
	*
	* @example
	* ```tsx
	* app.use('*', async (c, next) => {
	*   c.setRenderer((content) => {
	*     return c.html(
	*       <html>
	*         <body>
	*           <p>{content}</p>
	*         </body>
	*       </html>
	*     )
	*   })
	*   await next()
	* })
	* ```
	*/
	setRenderer = (renderer) => {
		this.#renderer = renderer;
	};
	/**
	* `.header()` can set headers.
	*
	* @see {@link https://hono.dev/docs/api/context#header}
	*
	* @example
	* ```ts
	* app.get('/welcome', (c) => {
	*   // Set headers
	*   c.header('X-Message', 'Hello!')
	*   c.header('Content-Type', 'text/plain')
	*
	*   // Append multiple headers using the append option (e.g. Vary)
	*   c.header('Vary', 'Accept-Encoding', { append: true })
	*   c.header('Vary', 'User-Agent', { append: true })
	*
	*   return c.body('Thank you for coming')
	* })
	* ```
	*/
	header = (name, value, options) => {
		if (this.finalized) this.#res = createResponseInstance(this.#res.body, this.#res);
		const headers = this.#res ? this.#res.headers : this.#preparedHeaders ??= new Headers();
		if (value === void 0) headers.delete(name);
		else if (options?.append) headers.append(name, value);
		else headers.set(name, value);
	};
	status = (status) => {
		this.#status = status;
	};
	/**
	* `.set()` can set the value specified by the key.
	*
	* @see {@link https://hono.dev/docs/api/context#set-get}
	*
	* @example
	* ```ts
	* app.use('*', async (c, next) => {
	*   c.set('message', 'Hono is hot!!')
	*   await next()
	* })
	* ```
	*/
	set = (key, value) => {
		this.#var ??= /* @__PURE__ */ new Map();
		this.#var.set(key, value);
	};
	/**
	* `.get()` can use the value specified by the key.
	*
	* @see {@link https://hono.dev/docs/api/context#set-get}
	*
	* @example
	* ```ts
	* app.get('/', (c) => {
	*   const message = c.get('message')
	*   return c.text(`The message is "${message}"`)
	* })
	* ```
	*/
	get = (key) => {
		return this.#var ? this.#var.get(key) : void 0;
	};
	/**
	* `.var` can access the value of a variable.
	*
	* @see {@link https://hono.dev/docs/api/context#var}
	*
	* @example
	* ```ts
	* const result = c.var.client.oneMethod()
	* ```
	*/
	get var() {
		if (!this.#var) return {};
		return Object.fromEntries(this.#var);
	}
	#newResponse(data, arg, headers) {
		let responseHeaders = this.#res ? new Headers(this.#res.headers) : this.#preparedHeaders;
		if (typeof arg === "object" && arg.headers) {
			responseHeaders ??= new Headers();
			for (const [key, value] of new Headers(arg.headers)) if (key === "set-cookie") responseHeaders.append(key, value);
			else responseHeaders.set(key, value);
		}
		if (headers) {
			if (!responseHeaders) {
				let count = 0;
				for (const k in headers) if (++count > 1 || typeof headers[k] !== "string") {
					responseHeaders = new Headers();
					break;
				}
			}
			if (responseHeaders) for (const k in headers) {
				const v = headers[k];
				if (typeof v === "string") responseHeaders.set(k, v);
				else {
					responseHeaders.delete(k);
					for (const v2 of v) responseHeaders.append(k, v2);
				}
			}
		}
		const status = typeof arg === "number" ? arg : arg?.status ?? this.#status;
		return createResponseInstance(data, {
			status,
			headers: responseHeaders ?? headers
		});
	}
	newResponse = (...args) => this.#newResponse(...args);
	/**
	* `.body()` can return the HTTP response.
	* You can set headers with `.header()` and set HTTP status code with `.status`.
	* This can also be set in `.text()`, `.json()` and so on.
	*
	* @see {@link https://hono.dev/docs/api/context#body}
	*
	* @example
	* ```ts
	* app.get('/welcome', (c) => {
	*   // Set headers
	*   c.header('X-Message', 'Hello!')
	*   c.header('Content-Type', 'text/plain')
	*   // Set HTTP status code
	*   c.status(201)
	*
	*   // Return the response body
	*   return c.body('Thank you for coming')
	* })
	* ```
	*/
	body = (data, arg, headers) => this.#newResponse(data, arg, headers);
	/**
	* `.text()` can render text as `Content-Type:text/plain`.
	*
	* @see {@link https://hono.dev/docs/api/context#text}
	*
	* @example
	* ```ts
	* app.get('/say', (c) => {
	*   return c.text('Hello!')
	* })
	* ```
	*/
	text = (text, arg, headers) => {
		return !this.#preparedHeaders && !this.#status && !arg && !headers && !this.finalized ? new Response(text) : this.#newResponse(text, arg, setDefaultContentType(TEXT_PLAIN, headers));
	};
	/**
	* `.json()` can render JSON as `Content-Type:application/json`.
	*
	* @see {@link https://hono.dev/docs/api/context#json}
	*
	* @example
	* ```ts
	* app.get('/api', (c) => {
	*   return c.json({ message: 'Hello!' })
	* })
	* ```
	*/
	json = (object, arg, headers) => {
		return this.#newResponse(JSON.stringify(object), arg, setDefaultContentType("application/json", headers));
	};
	html = (html, arg, headers) => {
		const res = (html) => this.#newResponse(html, arg, setDefaultContentType("text/html; charset=UTF-8", headers));
		return typeof html === "object" ? resolveCallback(html, HtmlEscapedCallbackPhase.Stringify, false, {}).then(res) : res(html);
	};
	/**
	* `.redirect()` can Redirect, default status code is 302.
	*
	* @see {@link https://hono.dev/docs/api/context#redirect}
	*
	* @example
	* ```ts
	* app.get('/redirect', (c) => {
	*   return c.redirect('/')
	* })
	* app.get('/redirect-permanently', (c) => {
	*   return c.redirect('/', 301)
	* })
	* ```
	*/
	redirect = (location, status) => {
		const locationString = String(location);
		this.header("Location", !/[^\x00-\xFF]/.test(locationString) ? locationString : encodeURI(locationString));
		return this.newResponse(null, status ?? 302);
	};
	/**
	* `.notFound()` can return the Not Found Response.
	*
	* @see {@link https://hono.dev/docs/api/context#notfound}
	*
	* @example
	* ```ts
	* app.get('/notfound', (c) => {
	*   return c.notFound()
	* })
	* ```
	*/
	notFound = () => {
		this.#notFoundHandler ??= () => createResponseInstance();
		return this.#notFoundHandler(this);
	};
};

//#region src/compose.ts
/**
* Compose middleware functions into a single function based on `koa-compose` package.
*
* @template E - The environment type.
*
* @param {[[Function, unknown], unknown][] | [[Function]][]} middleware - An array of middleware functions and their corresponding parameters.
* @param {ErrorHandler<E>} [onError] - An optional error handler function.
* @param {NotFoundHandler<E>} [onNotFound] - An optional not-found handler function.
*
* @returns {(context: Context, next?: Next) => Promise<Context>} - A composed middleware function.
*/
const compose = (middleware, onError, onNotFound) => {
	return (context, next) => {
		let index = -1;
		return dispatch(0);
		/**
		* Dispatch the middleware functions.
		*
		* @param {number} i - The current index in the middleware array.
		*
		* @returns {Promise<Context>} - A promise that resolves to the context.
		*/
		async function dispatch(i) {
			if (i <= index) throw new Error("next() called multiple times");
			index = i;
			let res;
			let isError = false;
			let handler;
			if (middleware[i]) {
				handler = middleware[i][0][0];
				context.req.routeIndex = i;
			} else handler = i === middleware.length && next || void 0;
			if (handler) try {
				res = await handler(context, () => dispatch(i + 1));
			} catch (err) {
				if (err instanceof Error && onError) {
					context.error = err;
					res = await onError(err, context);
					isError = true;
				} else throw err;
			}
			else if (context.finalized === false && onNotFound) res = await onNotFound(context);
			if (res && (context.finalized === false || isError)) context.res = res;
			return context;
		}
	};
};

//#region src/router.ts
/**
* @module
* This module provides types definitions and variables for the routers.
*/
/**
* Constant representing all HTTP methods in uppercase.
*/
/**
* Array of supported HTTP methods.
*/
const METHODS = [
	"get",
	"post",
	"put",
	"delete",
	"options",
	"patch",
	"query"
];
/**
* Error message indicating that a route cannot be added because the matcher is already built.
*/
const MESSAGE_MATCHER_IS_ALREADY_BUILT = "Can not add a route since the matcher is already built.";
/**
* Error class representing an unsupported path error.
*/
var UnsupportedPathError = class extends Error {};

//#region src/utils/constants.ts
/**
* Constant used to mark a composed handler.
*/
const COMPOSED_HANDLER = "__COMPOSED_HANDLER";

//#region src/hono-base.ts
/**
* @module
* This module is the base module for the Hono object.
*/
const notFoundHandler = (c) => {
	return c.text("404 Not Found", 404);
};
const errorHandler$1 = (err, c) => {
	if ("getResponse" in err) {
		const res = err.getResponse();
		return c.newResponse(res.body, res);
	}
	console.error(err);
	return c.text("Internal Server Error", 500);
};
var Hono$1 = class Hono {
	get;
	post;
	put;
	delete;
	options;
	patch;
	query;
	all;
	on;
	use;
	router;
	getPath;
	_basePath = "/";
	#path = "/";
	routes = [];
	constructor(options = {}) {
		[...METHODS, "all"].forEach((method) => {
			this[method] = (args1, ...args) => {
				const methodName = method.toUpperCase();
				if (typeof args1 === "string") this.#path = args1;
				else this.#addRoute(methodName, this.#path, args1);
				args.forEach((handler) => {
					this.#addRoute(methodName, this.#path, handler);
				});
				return this;
			};
		});
		this.on = (method, path, ...handlers) => {
			for (const p of [path].flat()) {
				this.#path = p;
				for (const m of [method].flat()) {
					const methodName = m.toUpperCase();
					for (const handler of handlers) this.#addRoute(methodName, this.#path, handler);
				}
			}
			return this;
		};
		this.use = (arg1, ...handlers) => {
			if (typeof arg1 === "string") this.#path = arg1;
			else {
				this.#path = "*";
				handlers.unshift(arg1);
			}
			handlers.forEach((handler) => {
				this.#addRoute("ALL", this.#path, handler);
			});
			return this;
		};
		const { strict, ...optionsWithoutStrict } = options;
		Object.assign(this, optionsWithoutStrict);
		this.getPath = strict ?? true ? options.getPath ?? getPath : getPathNoStrict;
	}
	#clone() {
		const clone = new Hono({
			router: this.router,
			getPath: this.getPath
		});
		clone.errorHandler = this.errorHandler;
		clone.#notFoundHandler = this.#notFoundHandler;
		clone.routes = this.routes;
		return clone;
	}
	#notFoundHandler = notFoundHandler;
	errorHandler = errorHandler$1;
	/**
	* `.route()` allows grouping other Hono instance in routes.
	*
	* @see {@link https://hono.dev/docs/api/routing#grouping}
	*
	* @param {string} path - base Path
	* @param {Hono} app - other Hono instance
	* @returns {Hono} routed Hono instance
	*
	* @example
	* ```ts
	* const app = new Hono()
	* const app2 = new Hono()
	*
	* app2.get("/user", (c) => c.text("user"))
	* app.route("/api", app2) // GET /api/user
	* ```
	*/
	route(path, app) {
		const subApp = this.basePath(path);
		app.routes.map((r) => {
			let handler;
			if (app.errorHandler === errorHandler$1) handler = r.handler;
			else {
				handler = async (c, next) => (await compose([], app.errorHandler)(c, () => r.handler(c, next))).res;
				handler[COMPOSED_HANDLER] = r.handler;
			}
			subApp.#addRoute(r.method, r.path, handler, r.basePath);
		});
		return this;
	}
	/**
	* `.basePath()` allows base paths to be specified.
	*
	* @see {@link https://hono.dev/docs/api/routing#base-path}
	*
	* @param {string} path - base Path
	* @returns {Hono} changed Hono instance
	*
	* @example
	* ```ts
	* const api = new Hono().basePath('/api')
	* ```
	*/
	basePath(path) {
		const subApp = this.#clone();
		subApp._basePath = mergePath(this._basePath, path);
		return subApp;
	}
	/**
	* `.onError()` handles an error and returns a customized Response.
	*
	* @see {@link https://hono.dev/docs/api/hono#error-handling}
	*
	* @param {ErrorHandler} handler - request Handler for error
	* @returns {Hono} changed Hono instance
	*
	* @example
	* ```ts
	* app.onError((err, c) => {
	*   console.error(`${err}`)
	*   return c.text('Custom Error Message', 500)
	* })
	* ```
	*/
	onError = (handler) => {
		this.errorHandler = handler;
		return this;
	};
	/**
	* `.notFound()` allows you to customize a Not Found Response.
	*
	* @see {@link https://hono.dev/docs/api/hono#not-found}
	*
	* @param {NotFoundHandler} handler - request handler for not-found
	* @returns {Hono} changed Hono instance
	*
	* @example
	* ```ts
	* app.notFound((c) => {
	*   return c.text('Custom 404 Message', 404)
	* })
	* ```
	*/
	notFound = (handler) => {
		this.#notFoundHandler = handler;
		return this;
	};
	/**
	* `.mount()` allows you to mount applications built with other frameworks into your Hono application.
	*
	* @deprecated Use `mount()` from `hono/mount` instead. `.mount()` will be removed in v5.
	*
	* @see {@link https://hono.dev/docs/api/hono#mount}
	*
	* @param {string} path - base Path
	* @param {Function} applicationHandler - other Request Handler
	* @param {MountOptions} [options] - options of `.mount()`
	* @returns {Hono} mounted Hono instance
	*
	* @example
	* ```ts
	* import { Router as IttyRouter } from 'itty-router'
	* import { Hono } from 'hono'
	* // Create itty-router application
	* const ittyRouter = IttyRouter()
	* // GET /itty-router/hello
	* ittyRouter.get('/hello', () => new Response('Hello from itty-router'))
	*
	* const app = new Hono()
	* app.mount('/itty-router', ittyRouter.handle)
	* ```
	*
	* @example
	* ```ts
	* const app = new Hono()
	* // Send the request to another application without modification.
	* app.mount('/app', anotherApp, {
	*   replaceRequest: (req) => req,
	* })
	* ```
	*/
	mount(path, applicationHandler, options) {
		let replaceRequest;
		let optionHandler;
		if (options) {
			if (typeof options === "function") optionHandler = options;
			else {
				optionHandler = options.optionHandler;
				if (options.replaceRequest === false) replaceRequest = (request) => request;
				else replaceRequest = options.replaceRequest;
			}
		}
		const getOptions = optionHandler ? (c) => {
			const options = optionHandler(c);
			return Array.isArray(options) ? options : [options];
		} : (c) => {
			let executionContext = void 0;
			try {
				executionContext = c.executionCtx;
			} catch {}
			return [c.env, executionContext];
		};
		replaceRequest ||= (() => {
			const mergedPath = mergePath(this._basePath, path);
			const pathPrefixLength = mergedPath === "/" ? 0 : mergedPath.length;
			return (request) => {
				const url = new URL(request.url);
				url.pathname = this.getPath(request).slice(pathPrefixLength) || "/";
				return new Request(url, request);
			};
		})();
		const handler = async (c, next) => {
			const res = await applicationHandler(replaceRequest(c.req.raw), ...getOptions(c));
			if (res) return res;
			await next();
		};
		this.#addRoute("ALL", mergePath(path, "*"), handler);
		return this;
	}
	#addRoute(method, path, handler, baseRoutePath) {
		path = mergePath(this._basePath, path);
		const r = {
			basePath: baseRoutePath !== void 0 ? mergePath(this._basePath, baseRoutePath) : this._basePath,
			path,
			method,
			handler
		};
		this.router.add(method, path, [handler, r]);
		this.routes.push(r);
	}
	#handleError(err, c) {
		if (err instanceof Error) return this.errorHandler(err, c);
		throw err;
	}
	#dispatch(request, executionCtx, env, method) {
		if (method === "HEAD") return (async () => new Response(null, await this.#dispatch(request, executionCtx, env, "GET")))();
		const path = this.getPath(request, { env });
		const matchResult = this.router.match(method, path);
		const c = new Context(request, {
			path,
			matchResult,
			env,
			executionCtx,
			notFoundHandler: this.#notFoundHandler
		});
		if (matchResult[0].length === 1) {
			let res;
			try {
				res = matchResult[0][0][0][0](c, async () => {
					c.res = await this.#notFoundHandler(c);
				});
			} catch (err) {
				return this.#handleError(err, c);
			}
			return res instanceof Promise ? res.then((resolved) => resolved || (c.finalized ? c.res : this.#notFoundHandler(c))).catch((err) => this.#handleError(err, c)) : res ?? this.#notFoundHandler(c);
		}
		const composed = compose(matchResult[0], this.errorHandler, this.#notFoundHandler);
		return (async () => {
			try {
				const context = await composed(c);
				if (!context.finalized) throw new Error("Context is not finalized. Did you forget to return a Response object or `await next()`?");
				return context.res;
			} catch (err) {
				return this.#handleError(err, c);
			}
		})();
	}
	/**
	* `.fetch()` will be entry point of your app.
	*
	* @see {@link https://hono.dev/docs/api/hono#fetch}
	*
	* @param {Request} request - request Object of request
	* @param {Env} env - env Object
	* @param {ExecutionContext} executionCtx - context of execution
	* @returns {Response | Promise<Response>} response of request
	*
	*/
	fetch = (request, ...rest) => {
		return this.#dispatch(request, rest[1], rest[0], request.method);
	};
	/**
	* `.request()` is a useful method for testing.
	* You can pass a URL or pathname to send a GET request.
	* app will return a Response object.
	* ```ts
	* test('GET /hello is ok', async () => {
	*   const res = await app.request('/hello')
	*   expect(res.status).toBe(200)
	* })
	* ```
	* @see https://hono.dev/docs/api/hono#request
	*/
	request = (input, requestInit, Env, executionCtx) => {
		if (input instanceof Request) return this.fetch(requestInit ? new Request(input, requestInit) : input, Env, executionCtx);
		input = input.toString();
		return this.fetch(new Request(/^https?:\/\//.test(input) ? input : `http://localhost${mergePath("/", input)}`, requestInit), Env, executionCtx);
	};
	/**
	* `.fire()` automatically adds a global fetch event listener.
	* This can be useful for environments that adhere to the Service Worker API, such as non-ES module Cloudflare Workers.
	* @deprecated
	* Use `fire` from `hono/service-worker` instead.
	* ```ts
	* import { Hono } from 'hono'
	* import { fire } from 'hono/service-worker'
	*
	* const app = new Hono()
	* // ...
	* fire(app)
	* ```
	* @see https://hono.dev/docs/api/hono#fire
	* @see https://developer.mozilla.org/en-US/docs/Web/API/Service_Worker_API
	* @see https://developers.cloudflare.com/workers/reference/migrate-to-module-workers/
	*/
	fire = () => {
		addEventListener("fetch", (event) => {
			event.respondWith(this.#dispatch(event.request, event, void 0, event.request.method));
		});
	};
};

//#region src/router/utils.ts
const createNullObject = () => Object.create(null);

//#region src/router/reg-exp-router/matcher.ts
const emptyParam = [];
function match(method, path) {
	const matchers = this.buildAllMatchers();
	const match = ((method, path) => {
		const matcher = matchers[method] || matchers["ALL"];
		const staticMatch = matcher[2][path];
		if (staticMatch) return staticMatch;
		const match = path.match(matcher[0]);
		if (!match) return [[], emptyParam];
		const index = match.indexOf("", 1);
		return [matcher[1][index], match];
	});
	this.match = match;
	return match(method, path);
}

//#region src/router/reg-exp-router/node.ts
const LABEL_REG_EXP_STR = "[^/]+";
const TAIL_WILDCARD_REG_EXP_STR = "(?:|/.*)";
const PATH_ERROR = Symbol();
const regExpMetaChars = /* @__PURE__ */ new Set(".\\+*[^]$()");
/**
* Sort order:
* 1. literal
* 2. special pattern (e.g. :label{[0-9]+})
* 3. common label pattern (e.g. :label)
* 4. wildcard
*/
function compareKey(a, b) {
	if (a.length === 1) return b.length === 1 ? a < b ? -1 : 1 : -1;
	if (b.length === 1) return 1;
	if (a === ".*" || a === "(?:|/.*)") return b === "(?:|/.*)" ? -1 : 1;
	else if (b === ".*" || b === "(?:|/.*)") return -1;
	if (a === "[^/]+") return 1;
	else if (b === "[^/]+") return -1;
	return a.length === b.length ? a < b ? -1 : 1 : b.length - a.length;
}
var Node$1 = class Node {
	#index;
	#varIndex;
	#children = createNullObject();
	insert(tokens, index, paramMap, context, isStatic) {
		let node = this;
		for (let i = 0, len = tokens.length; i < len; i++) {
			const token = tokens[i];
			const pattern = token.length === 1 ? token === "*" ? i === len - 1 ? [
				"",
				"",
				".*"
			] : [
				"",
				"",
				LABEL_REG_EXP_STR
			] : null : token === "/*" ? [
				"",
				"",
				TAIL_WILDCARD_REG_EXP_STR
			] : token.match(/^\:([^\{\}]+)(?:\{(.+)\})?$/);
			let nextNode;
			if (pattern) {
				const name = pattern[1];
				let regexpStr = pattern[2] || "[^/]+";
				if (name && pattern[2]) {
					if (regexpStr === ".*") throw PATH_ERROR;
					regexpStr = regexpStr.replace(/^\((?!\?:)(?=[^)]+\)$)/, "(?:");
					if (/\((?!\?:)/.test(regexpStr)) throw PATH_ERROR;
					if (regexpStr.length === 1 && regExpMetaChars.has(regexpStr)) throw PATH_ERROR;
				}
				nextNode = node.#children[regexpStr];
				if (!nextNode) {
					if (regexpStr !== ".*" && regexpStr !== "(?:|/.*)") {
						for (const k in node.#children) if ((regexpStr.length > 1 || k.length > 1) && k !== ".*" && k !== "(?:|/.*)") throw PATH_ERROR;
					}
					nextNode = node.#children[regexpStr] = new Node();
				}
				if (name !== "") {
					nextNode.#varIndex ??= context.varIndex++;
					paramMap.push([name, nextNode.#varIndex]);
				}
			} else {
				nextNode = node.#children[token];
				if (!nextNode) {
					for (const k in node.#children) if (k.length > 1 && k !== ".*" && k !== "(?:|/.*)") throw PATH_ERROR;
					nextNode = node.#children[token] = new Node();
				}
			}
			node = nextNode;
		}
		if (node.#index !== void 0) throw PATH_ERROR;
		node.#index = isStatic ? -1 : index;
	}
	buildRegExpStr() {
		const strList = Object.keys(this.#children).sort(compareKey).map((k) => {
			const c = this.#children[k];
			const childStr = c.buildRegExpStr();
			return childStr === "" ? "" : (typeof c.#varIndex === "number" ? `(${k})@${c.#varIndex}` : regExpMetaChars.has(k) ? `\\${k}` : k) + childStr;
		}).filter(Boolean);
		if (typeof this.#index === "number" && this.#index !== -1) strList.unshift(`#${this.#index}`);
		if (strList.length === 0) return "";
		if (strList.length === 1) return strList[0];
		return "(?:" + strList.join("|") + ")";
	}
};

//#region src/router/reg-exp-router/trie.ts
var Trie = class {
	#context = { varIndex: 0 };
	#root = new Node$1();
	#index = 0;
	paths = createNullObject();
	insert(path, isStatic) {
		if (isStatic) {
			this.#root.insert(path.split(""), 0, [], this.#context, true);
			return;
		}
		const paramAssoc = [];
		const groups = [];
		let markedPath = path;
		for (let i = 0;;) {
			let replaced = false;
			markedPath = markedPath.replace(/\{[^}]+\}/g, (m) => {
				const mark = `@\\${i}`;
				groups[i] = [mark, m];
				i++;
				replaced = true;
				return mark;
			});
			if (!replaced) break;
		}
		/**
		*  - pattern (:label, :label{0-9]+}, ...)
		*  - /* wildcard
		*  - character
		*/
		const tokens = markedPath.match(/(?::[^\/]+)|(?:\/\*$)|./g) || [];
		for (let i = groups.length - 1; i >= 0; i--) {
			const [mark] = groups[i];
			for (let j = tokens.length - 1; j >= 0; j--) if (tokens[j].indexOf(mark) !== -1) {
				tokens[j] = tokens[j].replace(mark, groups[i][1]);
				break;
			}
		}
		this.#root.insert(tokens, this.#index, paramAssoc, this.#context, false);
		this.paths[path] = [this.#index++, paramAssoc];
	}
	buildRegExp() {
		let regexp = this.#root.buildRegExpStr();
		if (regexp === "") return [
			/^$/,
			[],
			[]
		];
		let captureIndex = 0;
		const indexReplacementMap = [];
		const paramReplacementMap = [];
		regexp = regexp.replace(/#(\d+)|@(\d+)|\.\*\$/g, (_, handlerIndex, paramIndex) => {
			if (handlerIndex !== void 0) {
				indexReplacementMap[++captureIndex] = Number(handlerIndex);
				return "$()";
			}
			if (paramIndex !== void 0) {
				paramReplacementMap[Number(paramIndex)] = ++captureIndex;
				return "";
			}
			return "";
		});
		return [
			new RegExp(`^${regexp}`),
			indexReplacementMap,
			paramReplacementMap
		];
	}
};

//#region src/router/reg-exp-router/router.ts
let wildcardRegExpCache = createNullObject();
function buildWildcardRegExp(path) {
	return wildcardRegExpCache[path] ??= new RegExp(`^${path.replace(/\/:[^/{}]+(?:\{\[\^\/]\+})?(?=[/{]|$)|\/?\*$|([.\\+*[^\]$()?{}|])/g, (match, metaChar) => metaChar ? `\\${metaChar}` : match === "/*" ? TAIL_WILDCARD_REG_EXP_STR : match === "*" ? ".*" : `/:${LABEL_REG_EXP_STR}`)}$`);
}
function findMiddleware(middleware, path) {
	for (const k of Object.keys(middleware).sort((a, b) => b.length - a.length)) if (buildWildcardRegExp(k).test(path)) return [...middleware[k]];
}
var RegExpRouter = class {
	name = "RegExpRouter";
	#middleware;
	#routes;
	#tries;
	constructor() {
		this.#middleware = { ["ALL"]: createNullObject() };
		this.#routes = { ["ALL"]: createNullObject() };
		this.#tries = { ["ALL"]: new Trie() };
	}
	#insertPath(method, path) {
		try {
			this.#tries[method].insert(path, !/\*|\/:/.test(path));
		} catch (e) {
			throw e === PATH_ERROR ? new UnsupportedPathError(path) : e;
		}
	}
	add(method, path, handler) {
		const middleware = this.#middleware;
		const routes = this.#routes;
		if (!middleware) throw new Error(MESSAGE_MATCHER_IS_ALREADY_BUILT);
		if (!middleware[method]) {
			this.#tries[method] = new Trie();
			for (const handlerMap of [middleware, routes]) {
				handlerMap[method] = createNullObject();
				for (const p in handlerMap["ALL"]) {
					handlerMap[method][p] = [...handlerMap["ALL"][p]];
					this.#insertPath(method, p);
				}
			}
		}
		if (path === "/*") path = "*";
		const methods = method === "ALL" ? Object.keys(middleware) : [method];
		if (/\*$/.test(path)) {
			const re = buildWildcardRegExp(path);
			for (const m of methods) if (!middleware[m][path]) {
				this.#insertPath(m, path);
				middleware[m][path] = findMiddleware(middleware[m], path) || findMiddleware(middleware["ALL"], path) || [];
			}
			for (const handlerMap of [middleware, routes]) for (const m of methods) for (const p in handlerMap[m]) re.test(p) && handlerMap[m][p].push([handler, path]);
			return;
		}
		const paths = checkOptionalParameter(path) || [path];
		for (const path of paths) for (const m of methods) {
			if (!routes[m][path]) {
				this.#insertPath(m, path);
				routes[m][path] = findMiddleware(middleware[m], path) || findMiddleware(middleware["ALL"], path) || [];
			}
			routes[m][path].push([handler, path]);
		}
	}
	match = match;
	buildAllMatchers() {
		const matchers = createNullObject();
		for (const method of Object.keys(this.#routes)) matchers[method] = this.#buildMatcher(method);
		this.#middleware = this.#routes = this.#tries = void 0;
		wildcardRegExpCache = createNullObject();
		return matchers;
	}
	#buildMatcher(method) {
		const middleware = this.#middleware[method];
		const routes = this.#routes[method];
		const trie = this.#tries[method];
		const staticMap = createNullObject();
		const handlerData = [];
		const [regexp, indexReplacementMap, paramReplacementMap] = trie.buildRegExp();
		for (const r of [middleware, routes]) for (const path in r) {
			const handlers = r[path];
			const pathData = trie.paths[path];
			if (!pathData) {
				staticMap[path] = [handlers.map(([h]) => [h, createNullObject()]), emptyParam];
				continue;
			}
			handlerData[pathData[0]] = handlers.map(([h, handlerPath]) => [h, trie.paths[handlerPath][1].reduceRight((map, [key], i) => {
				map[key] = paramReplacementMap[pathData[1][i][1]];
				return map;
			}, createNullObject())]);
		}
		return [
			regexp,
			indexReplacementMap.map((i) => handlerData[i]),
			staticMap
		];
	}
};

//#region src/router/smart-router/router.ts
var SmartRouter = class {
	name = "SmartRouter";
	#routers = [];
	#routes = [];
	constructor(init) {
		this.#routers = init.routers;
	}
	add(method, path, handler) {
		if (!this.#routes) throw new Error(MESSAGE_MATCHER_IS_ALREADY_BUILT);
		this.#routes.push([
			method,
			path,
			handler
		]);
	}
	match(method, path) {
		if (!this.#routes) throw new Error("Fatal error");
		const routers = this.#routers;
		const routes = this.#routes;
		const len = routers.length;
		let i = 0;
		let res;
		for (; i < len; i++) {
			const router = routers[i];
			try {
				for (let i = 0, len = routes.length; i < len; i++) router.add(...routes[i]);
				res = router.match(method, path);
			} catch (e) {
				if (e instanceof UnsupportedPathError) continue;
				throw e;
			}
			this.match = router.match.bind(router);
			this.#routers = [router];
			this.#routes = void 0;
			break;
		}
		if (i === len) throw new Error("Fatal error");
		this.name = `SmartRouter + ${this.activeRouter.name}`;
		return res;
	}
	get activeRouter() {
		if (this.#routes || this.#routers.length !== 1) throw new Error("No active router has been determined yet.");
		return this.#routers[0];
	}
};

//#region src/router/trie-router/node.ts
const emptyParams = createNullObject();
let order = 0;
var Node = class Node {
	#methods = [];
	#children = createNullObject();
	#patterns = [];
	#pattern;
	#params = emptyParams;
	insert(method, path, handler) {
		let curNode = this;
		const parts = splitRoutingPath(path);
		const possibleKeys = /* @__PURE__ */ new Set();
		let i = 0;
		for (const p of parts) {
			const nextP = parts[++i];
			const pattern = getPattern(p, nextP) || (nextP === void 0 && p && p.indexOf("*") === p.length - 1 ? p : null);
			const isParam = Array.isArray(pattern);
			const key = isParam ? pattern[0] : pattern || p;
			const child = curNode.#children[key] ||= new Node();
			if (pattern && !child.#pattern) {
				child.#pattern = pattern;
				curNode.#patterns.push(child);
			}
			curNode = child;
			if (isParam) possibleKeys.add(pattern[1]);
		}
		curNode.#methods.push({ [method]: {
			handler,
			possibleKeys: [...possibleKeys],
			score: ++order
		} });
	}
	#pushHandlerSets(handlerSets, node, method, nodeParams, params) {
		for (let i = 0, len = node.#methods.length; i < len; i++) {
			const m = node.#methods[i];
			const handlerSet = m[method] || m["ALL"];
			if (handlerSet) {
				handlerSet.params = createNullObject();
				handlerSets.push(handlerSet);
				for (let i = 0, len = handlerSet.possibleKeys.length; i < len; i++) {
					const key = handlerSet.possibleKeys[i];
					handlerSet.params[key] = params?.[key] && !i ? params[key] : nodeParams[key] ?? params?.[key];
				}
			}
		}
	}
	search(method, path) {
		const handlerSets = [];
		this.#params = emptyParams;
		let curNodes = [this];
		const parts = splitPath(path);
		const curNodesQueue = [];
		const len = parts.length;
		let partOffsets = null;
		for (let i = 0; i < len; i++) {
			const part = parts[i];
			const isLast = i === len - 1;
			const tempNodes = [];
			for (let j = 0, len2 = curNodes.length; j < len2; j++) {
				const node = curNodes[j];
				const nextNode = node.#children[part];
				if (nextNode) {
					nextNode.#params = node.#params;
					if (isLast) {
						if (nextNode.#children["*"]) this.#pushHandlerSets(handlerSets, nextNode.#children["*"], method, node.#params);
						this.#pushHandlerSets(handlerSets, nextNode, method, node.#params);
					} else tempNodes.push(nextNode);
				}
				for (const child of node.#patterns) {
					const pattern = child.#pattern;
					const params = node.#params === emptyParams ? {} : { ...node.#params };
					if (typeof pattern === "string") {
						if (pattern === "*" || part.startsWith(pattern.slice(0, -1))) {
							this.#pushHandlerSets(handlerSets, child, method, node.#params);
							if (pattern === "*") {
								child.#params = params;
								tempNodes.push(child);
							}
						}
						continue;
					}
					const [, name, matcher] = pattern;
					if (!part && matcher === true) continue;
					if (matcher !== true) {
						if (!partOffsets) {
							partOffsets = [];
							let offset = path[0] === "/" ? 1 : 0;
							for (let p = 0; p < len; p++) {
								partOffsets[p] = offset;
								offset += parts[p].length + 1;
							}
						}
						const restPathString = path.slice(partOffsets[i]);
						const m = matcher.exec(restPathString);
						if (m) {
							params[name] = m[0];
							this.#pushHandlerSets(handlerSets, child, method, node.#params, params);
							if (m[0].length === restPathString.length && child.#children["*"]) this.#pushHandlerSets(handlerSets, child.#children["*"], method, node.#params, params);
							for (const _ in child.#children) {
								child.#params = params;
								const componentCount = m[0].match(/\//g)?.length ?? 0;
								(curNodesQueue[componentCount] ||= []).push(child);
								break;
							}
							continue;
						}
					}
					if (matcher === true || matcher.test(part)) {
						params[name] = part;
						if (isLast) {
							this.#pushHandlerSets(handlerSets, child, method, params, node.#params);
							if (child.#children["*"]) this.#pushHandlerSets(handlerSets, child.#children["*"], method, params, node.#params);
						} else {
							child.#params = params;
							tempNodes.push(child);
						}
					}
				}
			}
			const shifted = curNodesQueue.shift();
			curNodes = shifted ? tempNodes.concat(shifted) : tempNodes;
		}
		if (handlerSets[1]) handlerSets.sort((a, b) => {
			return a.score - b.score;
		});
		return [handlerSets.map(({ handler, params }) => [handler, params])];
	}
};

//#region src/router/trie-router/router.ts
var TrieRouter = class {
	name = "TrieRouter";
	#node = new Node();
	add(method, path, handler) {
		for (const result of checkOptionalParameter(path) || [path]) this.#node.insert(method, result, handler);
	}
	match(method, path) {
		return this.#node.search(method, path);
	}
};

//#region src/hono.ts
/**
* The Hono class extends the functionality of the HonoBase class.
* It sets up routing and allows for custom options to be passed.
*
* @template E - The environment type.
* @template S - The schema type.
* @template BasePath - The base path type.
*/
var Hono = class extends Hono$1 {
	/**
	* Creates an instance of the Hono class.
	*
	* @param options - Optional configuration options for the Hono instance.
	*/
	constructor(options = {}) {
		super(options);
		this.router = options.router ?? new SmartRouter({ routers: [new RegExpRouter(), new TrieRouter()] });
	}
};

//#region src/server/server-adapter/routes/permissions.ts
/**
* Map HTTP methods to permission actions.
* POST is context-dependent (write for data, execute for operations).
*/
const METHOD_TO_ACTION = {
	GET: "read",
	POST: "write",
	PUT: "write",
	PATCH: "write",
	DELETE: "delete"
};
/**
* Path patterns that indicate an "execute" action rather than "write" for POST requests.
* These are typically operation endpoints rather than data creation endpoints.
*/
const EXECUTE_PATTERNS = [
	"/generate",
	"/stream",
	"/execute",
	"/start",
	"/resume",
	"/restart",
	"/cancel",
	"/approve",
	"/decline",
	"/speak",
	"/listen",
	"/query",
	"/search",
	"/observe",
	"/time-travel",
	"/enhance",
	"/clone"
];
const PUBLISH_PATTERNS = [
	"/publish",
	"/activate",
	"/restore"
];
/**
* Maps `/stored/<family>` URL segments to canonical permission resource slugs.
*/
const STORED_RESOURCE_SEGMENTS = {
	agents: "stored-agents",
	"mcp-clients": "stored-mcp-clients",
	"prompt-blocks": "stored-prompt-blocks",
	scorers: "stored-scorers",
	skills: "stored-skills",
	workspaces: "stored-workspaces"
};
/**
* Extracts the primary resource name from a route path.
*
* The resource is derived from the first path segment, with special handling
* for compound resources and well-known paths.
*
* Note: The canonical list of resources is generated in permissions.generated.ts
* from SERVER_ROUTES via `pnpm generate:permissions`.
*
* @param path - The route path (e.g., '/agents/:agentId/generate')
* @returns The resource name (e.g., 'agents') or null if not identifiable
*
* @example
* extractResource('/agents/:agentId') // → 'agents'
* extractResource('/memory/threads/:threadId') // → 'memory'
* extractResource('/stored/agents/:agentId') // → 'stored-agents'
* extractResource('/stored/skills/:skillId') // → 'stored-skills'
*/
function extractResource(path) {
	const segments = path.replace(/^\//, "").split("/");
	if (segments.length === 0) return null;
	const firstSegment = segments[0];
	if (firstSegment === "stored" && segments[1]) return STORED_RESOURCE_SEGMENTS[segments[1]] ?? null;
	if (firstSegment === ".well-known") return "a2a";
	return firstSegment || null;
}
/**
* Determines the action based on HTTP method and path context.
*
* @param method - HTTP method (GET, POST, PUT, DELETE, etc.)
* @param path - The route path for context
* @returns The action string (read, write, execute, delete)
*/
function deriveAction(method, path) {
	const upperMethod = method.toUpperCase();
	if (upperMethod === "POST") {
		if (path.startsWith("/stored/")) {
			if (PUBLISH_PATTERNS.some((pattern) => path.endsWith(pattern))) return "publish";
		}
		return EXECUTE_PATTERNS.some((pattern) => path.includes(pattern)) ? "execute" : "write";
	}
	return METHOD_TO_ACTION[upperMethod] || "read";
}
/**
* Derives a permission string from a route's path and method.
*
* Uses convention: `{resource}:{action}`
*
* @param route - The server route to derive permission for
* @returns The derived permission string, or null if cannot be derived
*
* @example
* derivePermission({ path: '/agents', method: 'GET' }) // → 'agents:read'
* derivePermission({ path: '/agents/:id/generate', method: 'POST' }) // → 'agents:execute'
* derivePermission({ path: '/workflows/:id', method: 'DELETE' }) // → 'workflows:delete'
*/
function derivePermission(route) {
	if (route.method === "ALL") return null;
	const resource = extractResource(route.path);
	if (!resource) return null;
	return `${resource}:${deriveAction(route.method, route.path)}`;
}
/**
* Gets the effective permission for a route.
*
* Priority:
* 1. Explicit requiresPermission on the route (string or string[])
* 2. Derived permission from path/method convention
* 3. null (no permission required - should only happen for public routes)
*
* When the route specifies an array of permissions, the user needs ANY ONE
* of them (logical OR). This is useful for routes that serve multiple
* resource types.
*
* @param route - The server route
* @returns The permission string, array of alternative permissions, or null
*/
function getEffectivePermission(route) {
	if (route.requiresAuth === false) return null;
	if (route.requiresPermission) return route.requiresPermission;
	return derivePermission(route);
}
//#endregion
//#region src/server/server-adapter/routes/fga-manifest.ts
function isProtectedFGARoute$1(route) {
	return route.requiresAuth !== false;
}
function getToolRoutePermission(path) {
	return path.includes("/execute") ? "tools:execute" : "tools:read";
}
function getAgentToolResourceId(agentId, toolId) {
	return `${agentId}:${toolId}`;
}
function getMCPToolResourceId(serverId, toolId) {
	return JSON.stringify([serverId, toolId]);
}
const STORED_ROUTE_FGA = {
	agents: {
		resourceType: "stored-agents",
		idParams: ["storedAgentId", "agentId"]
	},
	"mcp-clients": {
		resourceType: "stored-mcp-clients",
		idParams: ["storedMCPClientId", "mcpClientId"]
	},
	"prompt-blocks": {
		resourceType: "stored-prompt-blocks",
		idParams: ["storedPromptBlockId", "promptBlockId"]
	},
	scorers: {
		resourceType: "stored-scorers",
		idParams: ["storedScorerId", "scorerId"]
	},
	skills: {
		resourceType: "stored-skills",
		idParams: ["storedSkillId"]
	},
	workspaces: {
		resourceType: "stored-workspaces",
		idParams: ["storedWorkspaceId"]
	}
};
function getStoredResourceRouteFGAConfig(path, permission) {
	const match = path.match(/^\/stored\/([^/]+)/);
	if (!match?.[1]) return null;
	const config = STORED_ROUTE_FGA[match[1]];
	if (!config) return null;
	return {
		resourceType: config.resourceType,
		resourceId: (params, { requestContext }) => {
			for (const idParam of config.idParams) {
				const id = params[idParam];
				if (typeof id === "string" && id) return id;
			}
			const scopedResourceId = requestContext?.get(MASTRA_RESOURCE_ID_KEY);
			return typeof scopedResourceId === "string" && scopedResourceId ? scopedResourceId : config.resourceType;
		},
		permission
	};
}
function getBuiltInRouteFGAConfig(route) {
	if (!isProtectedFGARoute$1(route) || !route.path || !route.method) return null;
	const permission = getEffectivePermission(route);
	if (!permission) return null;
	const path = route.path;
	const storedRouteConfig = getStoredResourceRouteFGAConfig(path, permission);
	if (storedRouteConfig) return storedRouteConfig;
	if (path.startsWith("/agents/:agentId/tools/:toolId")) return {
		resourceType: "tool",
		resourceId: ({ agentId, toolId }) => getAgentToolResourceId(String(agentId), String(toolId)),
		permission: getToolRoutePermission(path)
	};
	if (path.startsWith("/agents/:agentId")) return {
		resourceType: "agent",
		resourceIdParam: "agentId",
		permission
	};
	if (path.startsWith("/workflows/:workflowId")) return {
		resourceType: "workflow",
		resourceIdParam: "workflowId",
		permission
	};
	if (path.startsWith("/tools/:toolId")) return {
		resourceType: "tool",
		resourceIdParam: "toolId",
		permission
	};
	if (path.startsWith("/mcp/:serverId/tools/:toolId")) return {
		resourceType: "tool",
		resourceId: ({ serverId, toolId }) => getMCPToolResourceId(String(serverId), String(toolId)),
		permission: getToolRoutePermission(path)
	};
	if (path.startsWith("/mcp/:serverId")) return {
		resourceType: "mcp",
		resourceIdParam: "serverId",
		permission
	};
	if (path.startsWith("/memory/threads/:threadId") || path.startsWith("/memory/network/threads/:threadId")) return {
		resourceType: "thread",
		resourceIdParam: "threadId",
		permission
	};
	if (path === "/memory/threads" || path === "/memory/network/threads") return {
		resourceType: "thread",
		resourceId: ({ threadId, resourceId }) => {
			if (typeof threadId === "string") return threadId;
			return typeof resourceId === "string" ? resourceId : void 0;
		},
		permission
	};
	if (path === "/memory/save-messages" || path === "/memory/network/save-messages") return {
		resourceType: "thread",
		resourceId: ({ messages }) => {
			if (!Array.isArray(messages)) return void 0;
			const threadId = messages.find((message) => message && typeof message === "object" && "threadId" in message)?.threadId;
			return typeof threadId === "string" ? threadId : void 0;
		},
		permission
	};
	if (path === "/v1/responses") return {
		resourceType: "agent",
		resourceIdParam: "agent_id",
		permission
	};
	if (path.startsWith("/v1/responses/:responseId")) return {
		resourceType: "response",
		resourceIdParam: "responseId",
		permission
	};
	if (path === "/v1/conversations") return {
		resourceType: "agent",
		resourceIdParam: "agent_id",
		permission
	};
	if (path.startsWith("/v1/conversations/:conversationId")) return {
		resourceType: "conversation",
		resourceIdParam: "conversationId",
		permission
	};
	return null;
}
//#endregion
//#region src/server/server-adapter/mcp-auth.ts
function normalizeScopes(value) {
	if (Array.isArray(value)) return value.filter((s) => typeof s === "string");
	if (typeof value === "string") return value.split(/[\s,]+/).filter(Boolean);
	return [];
}
function resolveClientId(user) {
	for (const key of [
		"id",
		"sub",
		"userId",
		"email"
	]) {
		const value = user[key];
		if (typeof value === "string" && value) return value;
	}
	return "";
}
/**
* Build the default `AuthInfo` from the Mastra request context.
*
* Returns `undefined` when the request carries no identity at all, so servers
* without auth keep behaving exactly as before.
*/
function buildMcpAuthInfoFromRequestContext(requestContext) {
	const user = requestContext.get(MASTRA_USER_KEY);
	const token = requestContext.get(MASTRA_AUTH_TOKEN_KEY);
	const hasUser = !!user && typeof user === "object";
	const hasToken = typeof token === "string" && token.length > 0;
	if (!hasUser && !hasToken) return void 0;
	const userRecord = hasUser ? user : {};
	return {
		token: hasToken ? token : "",
		clientId: resolveClientId(userRecord),
		scopes: normalizeScopes(userRecord.scopes ?? userRecord.scope ?? userRecord.permissions),
		...hasUser ? { extra: { user: userRecord } } : {}
	};
}
/**
* Populate `req.auth` on the Node request that is about to be handed to an MCP
* transport.
*
* Resolution order:
* 1. An already-set `req.auth` is left untouched.
* 2. A configured `setRequestAuth` hook runs and fully owns the result.
* 3. Otherwise the principal resolved by Mastra server auth is bridged from the
*    request context using the default mapping.
*/
async function applyMcpRequestAuth({ req, requestContext, setRequestAuth }) {
	const request = req;
	if (request.auth) return;
	if (!requestContext) return;
	if (setRequestAuth) {
		await setRequestAuth(req, requestContext);
		return;
	}
	const authInfo = buildMcpAuthInfoFromRequestContext(requestContext);
	if (authInfo) request.auth = authInfo;
}
//#endregion
//#region src/server/server-adapter/redact.ts
/**
* Redacts request data from a v2 format payload.
* Removes `metadata.request` and `output.steps[].request` from the payload.
*/
function redactV2Payload(payload) {
	const redactedPayload = { ...payload };
	if (redactedPayload.metadata && typeof redactedPayload.metadata === "object") {
		const { request, ...metadataRest } = redactedPayload.metadata;
		redactedPayload.metadata = metadataRest;
	}
	if (redactedPayload.output && typeof redactedPayload.output === "object") {
		const output = { ...redactedPayload.output };
		if (Array.isArray(output.steps)) output.steps = output.steps.map((step) => {
			if (step && typeof step === "object") {
				const { request, ...stepRest } = step;
				return stepRest;
			}
			return step;
		});
		redactedPayload.output = output;
	}
	return redactedPayload;
}
/**
* Redacts sensitive data from stream chunks before they are sent to clients.
*
* This function strips out request bodies that may contain sensitive information
* such as system prompts, tool definitions, API keys, and other configuration data.
*
* Handles both v1 (legacy) and v2 stream formats:
*
* v1 format (fields at root level):
* - `step-start.request` - Contains the full LLM request body
* - `step-finish.request` - Contains the request body
* - `finish.request` - Contains the request body (if present)
*
* v2 format (fields nested in payload):
* - `step-start.payload.request` - Contains the full LLM request body
* - `step-finish.payload.metadata.request` - Contains the request metadata
* - `step-finish.payload.output.steps[].request` - Contains request data for each step
* - `finish.payload.metadata.request` - Contains the request metadata
* - `finish.payload.output.steps[].request` - Contains request data for each step
*
* @param chunk - The stream chunk to redact
* @returns A new chunk with sensitive data removed, or the original chunk if no redaction needed
*/
function redactStreamChunk(chunk) {
	if (!chunk || typeof chunk !== "object") return chunk;
	const typedChunk = chunk;
	switch (chunk.type) {
		case "step-start":
			if ("payload" in typedChunk && typedChunk.payload && typeof typedChunk.payload === "object") {
				const { payload, ...rest } = typedChunk;
				const { request, ...payloadRest } = payload;
				return {
					...rest,
					type: "step-start",
					payload: {
						...payloadRest,
						request: {}
					}
				};
			} else if ("request" in typedChunk) {
				const { request, ...rest } = typedChunk;
				return {
					...rest,
					type: "step-start",
					request: {}
				};
			}
			return chunk;
		case "step-finish":
			if ("payload" in typedChunk && typedChunk.payload && typeof typedChunk.payload === "object") {
				const { payload, ...rest } = typedChunk;
				return {
					...rest,
					type: "step-finish",
					payload: redactV2Payload(payload)
				};
			} else if ("request" in typedChunk) {
				const { request, ...rest } = typedChunk;
				return {
					...rest,
					type: "step-finish"
				};
			}
			return chunk;
		case "finish":
			if ("payload" in typedChunk && typedChunk.payload && typeof typedChunk.payload === "object") {
				const { payload, ...rest } = typedChunk;
				return {
					...rest,
					type: "finish",
					payload: redactV2Payload(payload)
				};
			} else if ("request" in typedChunk) {
				const { request, ...rest } = typedChunk;
				return {
					...rest,
					type: "finish"
				};
			}
			return chunk;
		default: return chunk;
	}
}
//#endregion
//#region src/server/server-adapter/serialize.ts
/**
* Serializes a stream chunk to JSON for wire transport.
*
* Stream chunks can contain values that plain `JSON.stringify` throws on
* (e.g. BigInt produced by zod coercions/transforms in structuredOutput
* schemas, or circular references) — `safeStringify` handles those. A single
* bad chunk must never kill the whole stream — Studio relies on later chunks
* (`workflow-step-result`, `workflow-finish`) to render run state.
*
* Returns `{ ok: false, error }` if the chunk cannot be serialized at all
* (e.g. a throwing getter/toJSON) — callers should log the error, skip the
* chunk, and keep streaming.
*/
function serializeStreamChunk(chunk) {
	try {
		return {
			ok: true,
			json: safeStringify(chunk)
		};
	} catch (error) {
		return {
			ok: false,
			error: error instanceof Error ? error : new Error(String(error))
		};
	}
}
//#endregion
//#region src/server/server-adapter/index.ts
function getCustomHTTPExceptionResponse(error) {
	if (!(error instanceof HTTPException$1) || !error.res) return;
	return error.getResponse();
}
/**
* Hono/adapter context key set by the framework-public middleware.
*
* When true, the current request targets a route the framework has declared
* public (via `createPublicRoute()` / `requiresAuth: false`). Adapter authors
* MUST short-circuit any user-registered middleware for such requests so that
* users cannot accidentally (or intentionally) 401 routes the framework needs
* to keep reachable (e.g. Studio sign-in endpoints).
*/
const MASTRA_FRAMEWORK_PUBLIC_KEY = "__mastraFrameworkPublic";
const AGENT_CHANNEL_WEBHOOK_PATH = /^\/api\/agents\/([^/]+)\/channels\/([^/]+)\/webhook\/?$/;
function isAbortSignalError(error) {
	if (!error || typeof error !== "object") return false;
	const { code, name } = error;
	return name === "AbortError" || code === "ABORT_ERR";
}
function isExpectedResponseCloseError(error) {
	if (!error || typeof error !== "object") return false;
	const { code } = error;
	return code === "ECONNRESET" || code === "EPIPE" || code === "ERR_STREAM_DESTROYED" || code === "ERR_STREAM_WRITE_AFTER_END" || code === "ERR_STREAM_PREMATURE_CLOSE";
}
function isResponseClosed(response) {
	return Boolean(response.writableEnded || response.destroyed);
}
function isProtectedFGARoute(route) {
	return route.requiresAuth !== false;
}
function formatRoute(route) {
	return `${route.method} ${route.path}`;
}
function isSchemaApiRoute$1(route) {
	return "_mastraSchemaRoute" in route && route._mastraSchemaRoute === true;
}
function getFGAProvider(mastra, requestContext) {
	if (requestContext) {
		if (requestContext.get("mastra__authMode") === "studio") {
			const studioFga = mastra?.getStudio?.()?.fga;
			if (studioFga) return studioFga;
		}
	}
	return mastra?.getServer?.()?.fga;
}
function getFGARouteInfo(route) {
	return {
		path: route.path,
		method: route.method,
		requiresAuth: route.requiresAuth,
		requiresPermission: route.requiresPermission,
		fga: route.fga
	};
}
function getRoutePermissions(route) {
	return [getEffectivePermission(route), route.fga?.permission].flatMap((value) => Array.isArray(value) ? value : [value]).filter((permission) => Boolean(permission));
}
async function resolveRouteFGAConfig(fgaProvider, route, requestContext, params) {
	if (route.fga) return route.fga;
	const resolvedConfig = await fgaProvider.resolveRouteFGA?.({
		route: getFGARouteInfo(route),
		params,
		requestContext
	});
	if (resolvedConfig) return resolvedConfig;
	return getBuiltInRouteFGAConfig(route);
}
function getSchemaTypeName(schema) {
	const schemaDef = schema?._def ?? schema?.def;
	return schemaDef?.typeName ?? schemaDef?.type;
}
function unwrapOptionalNullable(schema) {
	let inner = schema;
	let typeName = getSchemaTypeName(inner);
	while (typeName === "ZodOptional" || typeName === "ZodNullable" || typeName === "optional" || typeName === "nullable") {
		const innerDef = inner?._def ?? inner?.def;
		if (!innerDef?.innerType) return inner;
		inner = innerDef.innerType;
		typeName = getSchemaTypeName(inner);
	}
	return inner;
}
function parseComplexQueryParams(queryParamSchema, params) {
	if (!(queryParamSchema instanceof z$1.ZodObject)) return params;
	const parsedParams = { ...params };
	const shape = queryParamSchema.shape;
	for (const [key, fieldSchema] of Object.entries(shape)) {
		const rawValue = parsedParams[key];
		if (typeof rawValue !== "string") continue;
		const typeName = getSchemaTypeName(unwrapOptionalNullable(fieldSchema));
		if (!(typeName === "ZodObject" || typeName === "ZodArray" || typeName === "ZodRecord" || typeName === "object" || typeName === "array" || typeName === "record")) continue;
		try {
			parsedParams[key] = JSON.parse(rawValue);
		} catch {}
	}
	return parsedParams;
}
/**
* Property names that must never be written onto objects built from untrusted
* input: assigning them can mutate the object's prototype chain.
*/
const UNSAFE_OBJECT_KEYS = /* @__PURE__ */ new Set([
	"__proto__",
	"prototype",
	"constructor"
]);
/**
* Returns whether `key` is safe to use as a property name when building an
* object from untrusted input (query strings, form fields, etc.).
*/
function isSafeObjectKey(key) {
	return !UNSAFE_OBJECT_KEYS.has(key);
}
/**
* Normalizes query parameters from various HTTP framework formats to a consistent structure.
* Handles both single string values and arrays (for repeated query params like ?tag=a&tag=b).
* Reconstructs bracket-notation keys (e.g., `orderBy[field]=createdAt`) into JSON strings
* so that z.preprocess JSON.parse can handle them.
* Filters out non-string values that some frameworks may include.
*
* @param rawQuery - Raw query parameters from the HTTP framework (may contain strings, arrays, or nested objects)
* @returns Normalized query parameters as Record<string, string | string[]>
*/
function normalizeQueryParams(rawQuery) {
	const queryParams = {};
	const bracketGroups = {};
	for (const [key, value] of Object.entries(rawQuery)) {
		const bracketMatch = key.match(/^([^[]+)\[([^\]]+)\]$/);
		if (bracketMatch) {
			const parent = bracketMatch[1];
			const child = bracketMatch[2];
			const strValue = Array.isArray(value) ? value.filter((v) => typeof v === "string")[0] : typeof value === "string" ? value : void 0;
			if (strValue !== void 0 && isSafeObjectKey(parent) && isSafeObjectKey(child)) {
				if (!Object.hasOwn(bracketGroups, parent)) bracketGroups[parent] = {};
				bracketGroups[parent][child] = strValue;
			}
		} else if (typeof value === "string") {
			if (isSafeObjectKey(key)) queryParams[key] = value;
		} else if (Array.isArray(value)) {
			if (!isSafeObjectKey(key)) continue;
			const stringValues = value.filter((v) => typeof v === "string");
			queryParams[key] = stringValues.length === 1 ? stringValues[0] : stringValues;
		}
	}
	for (const [parent, children] of Object.entries(bracketGroups)) if (!Object.hasOwn(queryParams, parent)) queryParams[parent] = JSON.stringify(children);
	return queryParams;
}
/**
* Abstract base class for server adapters that handle HTTP requests.
*
* This class extends `MastraServerBase` to inherit app storage functionality
* and provides the framework for registering routes, middleware, and handling requests.
*
* Framework-specific adapters in @mastra/hono and @mastra/express extend this class
* (both named `MastraServer` in their respective packages) and implement the abstract
* methods for their specific framework.
*
* @template TApp - The type of the server app (e.g., Hono, Express Application)
* @template TRequest - The type of the request object
* @template TResponse - The type of the response object
*/
var MastraServer = class MastraServer extends MastraServerBase {
	/** Request bodies cannot set agent scopes: handlers strip `scopes`, and `mastra__scopes` is a reserved key. */
	reservesAgentScopes = true;
	mastra;
	bodyLimitOptions;
	tools;
	prefix;
	openapiPath;
	taskStore;
	customRouteAuthConfig;
	streamOptions;
	httpLoggingConfig;
	customApiRoutes;
	mcpOptions;
	customRouteHandler = null;
	/**
	* Whether custom (non-schema) API routes were registered. Adapters use this
	* at request time to decide if they need to preserve a pristine copy of the
	* request body for the custom-route bridge.
	*/
	get hasCustomRouteHandler() {
		return this.customRouteHandler !== null;
	}
	constructor({ app, mastra, bodyLimitOptions, tools, prefix = "/api", openapiPath = "", taskStore, customRouteAuthConfig, streamOptions, customApiRoutes, mcpOptions }) {
		super({
			app,
			name: "MastraServer"
		});
		this.mastra = mastra;
		this.bodyLimitOptions = bodyLimitOptions;
		this.tools = tools;
		this.prefix = normalizeRoutePath(prefix);
		this.openapiPath = openapiPath;
		this.taskStore = taskStore;
		this.customRouteAuthConfig = customRouteAuthConfig;
		this.streamOptions = {
			redact: true,
			...streamOptions
		};
		this.customApiRoutes = customApiRoutes;
		this.mcpOptions = mcpOptions;
		const serverConfig = mastra.getServer();
		this.httpLoggingConfig = this.parseLoggingConfig(serverConfig?.build?.apiReqLogs);
		mastra.setMastraServer(this);
	}
	/**
	* Parses the apiReqLogs configuration into a normalized HttpLoggingConfig.
	* @param config - The raw config value from server.build.apiReqLogs
	* @returns Normalized HttpLoggingConfig or undefined if disabled
	*/
	parseLoggingConfig(config) {
		if (config === true) return {
			enabled: true,
			level: "info",
			redactHeaders: ["authorization", "cookie"]
		};
		if (typeof config === "object" && config.enabled) return {
			enabled: true,
			level: config.level || "info",
			excludePaths: config.excludePaths,
			includeHeaders: config.includeHeaders,
			includeQueryParams: config.includeQueryParams,
			redactHeaders: [.../* @__PURE__ */ new Set([...["authorization", "cookie"], ...config.redactHeaders || []])]
		};
	}
	/**
	* Determines if a request to the given path should be logged.
	* @param path - The request path to check
	* @returns true if the request should be logged, false otherwise
	*/
	shouldLogRequest(path) {
		if (!this.httpLoggingConfig?.enabled) return false;
		return !(this.httpLoggingConfig.excludePaths || []).some((excluded) => path === excluded || path.startsWith(excluded + "/"));
	}
	/** Warn when a request looks like an agent channel webhook whose route was never registered. */
	warnIfUnregisteredChannelWebhook(path, method, status) {
		if (status !== 404 || method.toUpperCase() !== "POST") return;
		const match = AGENT_CHANNEL_WEBHOOK_PATH.exec(path);
		if (!match) return;
		const routes = this.customApiRoutes ?? this.mastra.getServer()?.apiRoutes;
		if (findMatchingCustomRoute(path, method.toUpperCase(), routes)) return;
		const agentId = match[1];
		const platform = match[2];
		const platformLabel = /^[a-z0-9]+(?:[-_][a-z0-9]+)*$/i.test(platform) ? platform.split(/[-_]/).map((segment) => segment.charAt(0).toUpperCase() + segment.slice(1)).join(" ") : "channel";
		this.mastra.getLogger()?.warn(`Received a ${platformLabel} webhook, but this agent doesn't have a ${platformLabel} adapter. Add one to the agent's channels.adapters configuration and restart the server.`, {
			agentId,
			platform
		});
	}
	mergeRequestContext({ paramsRequestContext, bodyRequestContext }) {
		const requestContext = new RequestContext();
		if (bodyRequestContext) for (const [key, value] of Object.entries(bodyRequestContext)) {
			if (isReservedRequestContextKey(key)) continue;
			requestContext.set(key, value);
		}
		if (paramsRequestContext) for (const [key, value] of Object.entries(paramsRequestContext)) {
			if (isReservedRequestContextKey(key)) continue;
			requestContext.set(key, value);
		}
		return requestContext;
	}
	applyRequestMetadataToContext({ requestContext, getHeader }) {
		if (isStudioClientTypeHeader(getHeader("x-mastra-client-type"))) requestContext.set(MASTRA_IS_STUDIO_KEY, true);
	}
	/**
	* Determines which auth configuration to use for the current request.
	*
	* Request routing logic:
	* 1. If `x-mastra-client-type: studio` header is present AND `studio.auth` is configured:
	*    → Use studio auth (for internal team members accessing Studio UI)
	* 2. If studio header is present but `studio.auth` is NOT configured:
	*    → No auth required (Studio development mode)
	* 3. Otherwise:
	*    → Use server auth (for external customers calling API)
	*
	* Security note: The header is only for routing - auth validation happens
	* via session cookies/tokens. If someone spoofs the studio header but doesn't
	* have a valid studio session, they'll get a 401 (not fall back to server auth).
	*/
	getEffectiveAuthConfig(getHeader) {
		const isStudioRequest = isStudioClientTypeHeader(getHeader(MASTRA_CLIENT_TYPE_HEADER));
		const studioAuth = this.mastra.getStudio?.()?.auth;
		const serverAuth = this.mastra.getServer()?.auth;
		if (isStudioRequest && studioAuth) return {
			authConfig: studioAuth,
			authMode: "studio"
		};
		if (serverAuth) return {
			authConfig: serverAuth,
			authMode: "server"
		};
		return null;
	}
	/**
	* Gets the effective RBAC provider for the current request based on auth mode.
	*/
	getEffectiveRBACProvider(requestContext) {
		if (requestContext.get("mastra__authMode") === "studio") return this.mastra.getStudio?.()?.rbac ?? this.mastra.getServer()?.rbac;
		return this.mastra.getServer()?.rbac;
	}
	/**
	* Gets the effective FGA provider for the current request based on auth mode.
	*/
	getEffectiveFGAProvider(requestContext) {
		if (requestContext.get("mastra__authMode") === "studio") return this.mastra.getStudio?.()?.fga ?? this.mastra.getServer()?.fga;
		return this.mastra.getServer()?.fga;
	}
	/**
	* Check if the current request should be authenticated/authorized.
	* Returns null if auth passes, or an error response if it fails.
	*
	* This is a thin wrapper around coreAuthMiddleware that:
	* 1. Routes to the correct auth provider (studio vs server) based on request headers
	* 2. Handles route-level requiresAuth opt-out (not available in global middleware)
	* 3. Delegates all other auth logic to coreAuthMiddleware
	* 4. Translates the AuthResult into the {status, error} format adapters expect
	*
	* Security: When `x-mastra-client-type: studio` header is present and studio auth
	* is configured, we ONLY use studio auth. If authentication fails, we return 401
	* and redirect to login - we do NOT fall back to server auth. This prevents
	* external users from spoofing the studio header to access Studio UI.
	*/
	async checkRouteAuth(route, context) {
		const effectiveAuth = this.getEffectiveAuthConfig(context.getHeader);
		if (!effectiveAuth) return null;
		const { authConfig, authMode } = effectiveAuth;
		context.requestContext.set(MASTRA_AUTH_MODE_KEY, authMode);
		if (route.requiresAuth === false) return null;
		const authHeader = context.getHeader("authorization");
		let token = authHeader ? authHeader.replace("Bearer ", "") : null;
		if (!token) token = context.getQuery("apiKey") || null;
		const fallbackHeaders = new Headers();
		for (const headerName of ["authorization", "cookie"]) {
			const headerValue = context.getHeader(headerName);
			if (headerValue) fallbackHeaders.set(headerName, headerValue);
		}
		const result = await coreAuthMiddleware({
			path: context.path,
			method: context.method,
			getHeader: context.getHeader,
			mastra: this.mastra,
			authConfig,
			customRouteAuthConfig: this.customRouteAuthConfig,
			requestContext: context.requestContext,
			rawRequest: context.request ?? new Request(`http://localhost${context.path}`, {
				method: context.method,
				headers: fallbackHeaders
			}),
			token,
			buildAuthorizeContext: context.buildAuthorizeContext ?? (() => null),
			requiresAuth: route.requiresAuth
		});
		if (result.action === "next") {
			if (result.headers) return {
				status: 200,
				error: "",
				headers: result.headers
			};
			return null;
		}
		const errorBody = result.body;
		return {
			status: result.status,
			error: errorBody?.error ?? "Access denied",
			headers: result.headers
		};
	}
	/**
	* Check if the user has the required permission for a route.
	*
	* Uses convention-based permission derivation:
	* 1. If route has explicit `requiresPermission`, use that
	* 2. Otherwise, derive permission from path/method (e.g., GET /agents → agents:read)
	* 3. Routes with `requiresAuth: false` skip permission checks
	*
	* When the route specifies an array of permissions, the user needs ANY ONE
	* of them (logical OR).
	*
	* @param route - The route being accessed
	* @param userPermissions - The user's permissions from the request context
	* @returns Error response if permission denied, null if allowed
	*/
	/**
	* Check if the user has the required permission for a route.
	*
	* Uses convention-based permission derivation:
	* 1. If route has explicit `requiresPermission`, use that
	* 2. Otherwise, derive permission from path/method (e.g., GET /agents → agents:read)
	* 3. Routes with `requiresAuth: false` skip permission checks
	*
	* Permission checks use the RBAC provider that corresponds to the auth mode
	* (studio vs server) that was used for authentication.
	*
	* @param route - The route being accessed
	* @param userPermissions - The user's permissions from the request context
	* @param hasPermissionFn - Function to check if user permissions match required permission
	* @param requestContext - Request context to determine which RBAC provider to use
	* @returns Error response if permission denied, null if allowed
	*/
	checkRoutePermission(route, userPermissions, hasPermissionFn, requestContext) {
		if (!(requestContext ? this.getEffectiveRBACProvider(requestContext) : this.mastra.getServer()?.rbac)) return null;
		const requiredPermission = getEffectivePermission(route);
		if (!requiredPermission) return null;
		const permissions = Array.isArray(requiredPermission) ? requiredPermission : [requiredPermission];
		if (!(userPermissions && permissions.some((perm) => hasPermissionFn(userPermissions, perm)))) return {
			status: 403,
			error: "Forbidden",
			message: `Missing required permission: ${permissions.join(" or ")}`
		};
		return null;
	}
	/**
	* Build a matcher that answers "is this (path, method) a framework-public
	* route?" — i.e. registered with `requiresAuth: false`.
	*
	* Adapters call this once at registration time and use the returned function
	* inside their context middleware to stash a boolean on the per-request
	* context under `MASTRA_FRAMEWORK_PUBLIC_KEY`. That boolean is then read by
	* per-user-middleware wrappers to short-circuit user middleware for
	* framework-public routes, guaranteeing that user middleware cannot 401
	* routes the framework declared public (e.g. Studio sign-in endpoints).
	*
	* The matcher considers:
	*   - Built-in `SERVER_ROUTES` entries with `requiresAuth === false`
	*     (paths joined with the adapter's configured `prefix`).
	*   - Custom user API routes registered with `requiresAuth: false` via the
	*     `customRouteAuthConfig` map.
	*/
	getFrameworkPublicMatcher() {
		const prefix = this.prefix ?? "";
		const publicRoutes = SERVER_ROUTES.filter((r) => r.requiresAuth === false).map((r) => ({
			method: r.method.toUpperCase(),
			pattern: `${prefix}${r.path}`
		}));
		return (path, method) => {
			const upperMethod = method.toUpperCase();
			for (const r of publicRoutes) if ((r.method === upperMethod || r.method === "ALL") && pathMatchesPattern(path, r.pattern)) return true;
			return isCustomRoutePublic(path, upperMethod, this.customRouteAuthConfig);
		};
	}
	async init() {
		this.registerContextMiddleware();
		this.registerAuthMiddleware();
		this.validateAuthResourceScoping();
		this.registerUserMiddleware();
		this.registerHttpLoggingMiddleware();
		await this.validateEELicense();
		await this.validateAgentBuilderLicense();
		await this.validateFGAPolicyCoverage();
		await this.registerCustomApiRoutes();
		await this.registerRoutes();
	}
	/**
	* Validate that EE features have a valid license in production.
	* Throws if RBAC or FGA is configured without a valid license outside dev/test environments.
	*/
	async validateEELicense() {
		const serverConfig = this.mastra.getServer();
		const studioConfig = this.mastra.getStudio?.();
		const configuredFeatures = [serverConfig?.rbac || studioConfig?.rbac ? "RBAC" : null, serverConfig?.fga || studioConfig?.fga ? "FGA" : null].filter((feature) => feature !== null);
		if (configuredFeatures.length === 0) return;
		try {
			const { isEEEnabled } = await import('@mastra/core/auth/ee');
			if (!isEEEnabled()) {
				const featureList = configuredFeatures.join(" and ");
				throw new Error(`[mastra/auth-ee] ${featureList} ${configuredFeatures.length === 1 ? "is" : "are"} configured but no valid EE license was found.\n${featureList} ${configuredFeatures.length === 1 ? "requires" : "require"} a Mastra Enterprise License for production use.\nSet the MASTRA_EE_LICENSE environment variable with your license key.
Learn more: https://github.com/mastra-ai/mastra/blob/main/ee/LICENSE`);
			}
		} catch (err) {
			if (err instanceof Error && err.message.startsWith("[mastra/auth-ee]")) throw err;
			throw new Error(`[mastra/auth-ee] ${configuredFeatures.join(" and ")} ${configuredFeatures.length === 1 ? "is" : "are"} configured but the EE module (@mastra/core/auth/ee) could not be loaded.\nEnsure @mastra/core is updated to a version that includes EE support.`);
		}
	}
	/**
	* Validate that an Agent Builder configuration has a valid EE license.
	* Throws if the editor is configured with builder support but no valid EE license is available.
	*/
	async validateAgentBuilderLicense() {
		if (!this.mastra.getEditor()?.hasEnabledBuilderConfig?.()) return;
		try {
			const { isEEEnabled } = await import('@mastra/core/auth/ee');
			if (!isEEEnabled()) throw new Error("[mastra/auth-ee] Agent Builder is configured but no valid EE license was found.\nAgent Builder requires a Mastra Enterprise License for production use.\nSet the MASTRA_EE_LICENSE environment variable with your license key.\nLearn more: https://github.com/mastra-ai/mastra/blob/main/ee/LICENSE");
		} catch (err) {
			if (err instanceof Error && err.message.startsWith("[mastra/auth-ee]")) throw err;
			throw new Error("[mastra/auth-ee] Agent Builder is configured but the EE module (@mastra/core/auth/ee) could not be loaded.\nEnsure @mastra/core is updated to a version that includes EE support.");
		}
	}
	/**
	* Warn when `server.auth` is configured without `mapUserToResourceId`.
	*
	* Without it, MASTRA_RESOURCE_ID_KEY is never set in the request context, so
	* built-in routes fall back to the client-supplied resource ID (for example
	* `memory.resource`) and ownership checks run against a caller-chosen value.
	*/
	validateAuthResourceScoping() {
		const auth = this.mastra.getServer()?.auth;
		if (!auth || typeof auth.mapUserToResourceId === "function") return;
		this.mastra.getLogger()?.warn("[mastra/auth] server.auth is configured without mapUserToResourceId. Memory, thread and workflow ownership checks will trust the resource ID supplied by the client (e.g. memory.resource in the request body), so an authenticated user can read or write another user's threads. Configure server.auth.mapUserToResourceId to derive the resource ID from the authenticated user.");
	}
	/**
	* Validate route-level FGA policy coverage when an FGA provider opts into
	* startup checks.
	*/
	async validateFGAPolicyCoverage() {
		const serverConfig = this.mastra.getServer();
		const studioConfig = this.mastra.getStudio?.();
		const fgaProvider = serverConfig?.fga ?? studioConfig?.fga;
		if (!fgaProvider) return;
		const customRoutes = (this.customApiRoutes ?? serverConfig?.apiRoutes ?? []).filter((route) => !route._mastraInternal);
		const routes = [...SERVER_ROUTES, ...customRoutes];
		if (fgaProvider.validatePermissions) {
			const permissions = [...new Set(routes.flatMap((route) => getRoutePermissions(route)))];
			await fgaProvider.validatePermissions(permissions);
		}
		const auditMode = fgaProvider.auditProtectedRoutes ?? (fgaProvider.requireForProtectedRoutes ? "warn" : false);
		if (!auditMode || fgaProvider.resolveRouteFGA) return;
		const missingRoutes = routes.filter((route) => isProtectedFGARoute(route) && !route.fga && !getBuiltInRouteFGAConfig(route));
		if (missingRoutes.length === 0) return;
		const routeList = missingRoutes.map((route) => formatRoute(route));
		const message = `[mastra/auth-ee] FGA is configured but ${missingRoutes.length} protected route${missingRoutes.length === 1 ? " is" : "s are"} missing FGA metadata: ${routeList.join(", ")}`;
		if (auditMode === "error") throw new Error(message);
		this.mastra.getLogger()?.warn(message, {
			routes: routeList,
			count: missingRoutes.length
		});
	}
	/**
	* Register user-provided middleware from the Mastra config (`server.middleware`)
	* and from `mastra.setServerMiddleware()`. Called by init() between
	* registerAuthMiddleware() and registerHttpLoggingMiddleware().
	*
	* Mastra middleware handlers use Hono's `(c, next)` signature, so only
	* Hono-based adapters can run them. Those adapters override this method and
	* MUST wrap each handler with `skipIfFrameworkPublic` (exported by
	* `@mastra/hono`) so user middleware cannot block framework-public routes.
	* The default implementation warns when middleware is configured so the
	* config is not silently ignored on adapters that cannot honor it.
	*/
	registerUserMiddleware() {
		const instanceMiddleware = this.mastra.getServerMiddleware?.() ?? [];
		const configMiddleware = this.mastra.getServer()?.middleware;
		const hasConfigMiddleware = Array.isArray(configMiddleware) ? configMiddleware.length > 0 : !!configMiddleware;
		if (instanceMiddleware.length === 0 && !hasConfigMiddleware) return;
		this.mastra.getLogger()?.warn("Mastra server middleware (`server.middleware` or `setServerMiddleware()`) is configured, but this server adapter cannot run Hono middleware handlers. The configured middleware will not run — register middleware through your server framework instead.");
	}
	/**
	* Override in adapters to register custom API routes defined via registerApiRoute().
	* Called by init() between registerAuthMiddleware() and registerRoutes().
	*/
	async registerCustomApiRoutes() {}
	/** Ensures every custom route has an auth entry without overriding supplied values. */
	initializeCustomRouteAuthConfig(routes) {
		this.customRouteAuthConfig ??= /* @__PURE__ */ new Map();
		for (const route of routes) {
			const routeKey = `${route.method}:${route.path}`;
			if (!this.customRouteAuthConfig.has(routeKey)) this.customRouteAuthConfig.set(routeKey, route.requiresAuth !== false);
		}
	}
	/**
	* Registers schema-aware custom routes through the adapter's native route pipeline.
	* Returns the remaining Hono-style routes for the custom-route bridge.
	*/
	async registerSchemaApiRoutes() {
		const routes = this.customApiRoutes ?? this.mastra.getServer()?.apiRoutes ?? [];
		this.validateCustomRoutePaths(routes);
		this.initializeCustomRouteAuthConfig(routes);
		const schemaRoutes = routes.filter(isSchemaApiRoute$1);
		for (const route of schemaRoutes) await this.registerRoute(this.app, route, { prefix: "" });
		return routes.filter((route) => !isSchemaApiRoute$1(route));
	}
	/**
	* Validates that no custom route path collides with the built-in route prefix.
	* Throws if any route path starts with the server's `apiPrefix`.
	*/
	validateCustomRoutePaths(routes) {
		const prefix = this.prefix ?? "";
		if (!prefix) return;
		for (const route of routes) {
			if (route._mastraInternal) continue;
			if (route.path.startsWith(`${prefix}/`) || route.path === prefix) throw new Error(`Custom API route "${route.path}" must not start with "${prefix}" — that path is reserved for built-in Mastra routes. Choose a different path (e.g. "${route.path.replace(prefix, "/custom")}").`);
		}
	}
	/**
	* Creates an internal Hono sub-app with all custom API routes registered.
	* Stores the handler on this instance for use by handleCustomRouteRequest().
	* Returns true if custom routes were found and registered.
	*/
	async buildCustomRouteHandler(routes) {
		if (routes.length === 0) return false;
		const NOT_FOUND_HEADER = "x-mastra-custom-route-not-found";
		const mastra = this.mastra;
		const app = new Hono();
		app.use("*", async (c, next) => {
			c.set("mastra", mastra);
			c.set("requestContext", c.env?.requestContext ?? new RequestContext());
			await next();
		});
		const serverOnError = this.mastra.getServer()?.onError;
		app.onError((err, c) => {
			if (serverOnError) return serverOnError(err, c);
			this.mastra.getLogger()?.error(`Custom route handler failed: ${c.req.method} ${c.req.path}`, {
				method: c.req.method,
				path: c.req.path,
				error: err instanceof Error ? err.stack ?? err.message : String(err)
			});
			return c.json({ error: "Internal Server Error" }, 500);
		});
		for (const route of routes) {
			const handler = "handler" in route && route.handler ? route.handler : "createHandler" in route ? await route.createHandler({ mastra }) : void 0;
			if (!handler) continue;
			const middlewares = [];
			if (route.middleware) middlewares.push(...Array.isArray(route.middleware) ? route.middleware : [route.middleware]);
			const allHandlers = [...middlewares, handler];
			if (route.method === "ALL") app.all(route.path, allHandlers[0], ...allHandlers.slice(1));
			else app.on(route.method, route.path, allHandlers[0], ...allHandlers.slice(1));
		}
		app.notFound(() => new Response(null, {
			status: 404,
			headers: { [NOT_FOUND_HEADER]: "true" }
		}));
		this.customRouteHandler = async (request, env, executionCtx) => app.fetch(request, env, executionCtx);
		return true;
	}
	/**
	* Forwards a request to the internal custom route handler.
	* Returns the Response if a custom route matched, or null to fall through.
	*/
	async handleCustomRouteRequest(url, method, headers, body, requestContext, signal, executionCtx) {
		if (!this.customRouteHandler) return null;
		const fetchHeaders = new Headers();
		for (const [key, value] of Object.entries(headers)) if (typeof value === "string") fetchHeaders.set(key, value);
		else if (Array.isArray(value)) value.forEach((v) => {
			fetchHeaders.append(key, v);
		});
		const init = {
			method,
			headers: fetchHeaders,
			signal
		};
		if ([
			"POST",
			"PUT",
			"PATCH",
			"DELETE"
		].includes(method) && body !== void 0) {
			if (body instanceof ArrayBuffer || body instanceof Uint8Array || body instanceof ReadableStream) {
				init.body = body;
				if (body instanceof ReadableStream) init.duplex = "half";
			} else if (((typeof headers["content-type"] === "string" ? headers["content-type"] : "") || "").includes("application/json")) init.body = JSON.stringify(body);
			else if (typeof body === "string") init.body = body;
		}
		const request = new globalThis.Request(url, init);
		const response = await this.customRouteHandler(request, { requestContext }, executionCtx);
		if (response.headers.get("x-mastra-custom-route-not-found") === "true") return null;
		return response;
	}
	/**
	* Pipes a custom route Response to a Node.js ServerResponse (http.ServerResponse).
	* Works with Koa (ctx.res), Express (res), and Fastify (reply.raw).
	*/
	async writeCustomRouteResponse(response, nodeRes, signal) {
		const headers = {};
		response.headers.forEach((value, key) => {
			if (key.toLowerCase() !== "set-cookie") headers[key] = value;
		});
		const setCookies = response.headers.getSetCookie?.();
		if (setCookies && setCookies.length > 0) headers["set-cookie"] = setCookies;
		if (isResponseClosed(nodeRes)) {
			await response.body?.cancel();
			return;
		}
		nodeRes.writeHead(response.status, headers);
		if (response.body) {
			let responseBodyError;
			let responseBodyErrorAfterResponseClosed = false;
			try {
				const responseStream = Readable.fromWeb(response.body);
				responseStream.once("error", (error) => {
					responseBodyError = error;
					responseBodyErrorAfterResponseClosed = isResponseClosed(nodeRes);
				});
				if (signal) await pipeline(responseStream, nodeRes, { signal });
				else await pipeline(responseStream, nodeRes);
			} catch (error) {
				const expectedSignalAbort = signal?.aborted && isAbortSignalError(error) && (!responseBodyError || responseBodyErrorAfterResponseClosed);
				const expectedResponseClose = (!responseBodyError || responseBodyErrorAfterResponseClosed) && isResponseClosed(nodeRes) && isExpectedResponseCloseError(error);
				if (!expectedSignalAbort && !expectedResponseClose) throw error;
			}
		} else {
			const text = await response.text();
			if (!isResponseClosed(nodeRes)) nodeRes.end(text);
		}
	}
	/**
	* Builds the OpenAPI spec object with servers field and custom route paths.
	*/
	buildOpenAPISpec(config, prefix) {
		const openApiSpec = generateOpenAPIDocument(SERVER_ROUTES, config);
		if (prefix) openApiSpec.servers = [{ url: prefix }];
		const allCustomRoutes = this.customApiRoutes ?? this.mastra.getServer()?.apiRoutes;
		if (allCustomRoutes && allCustomRoutes.length > 0) {
			const customPaths = convertCustomRoutesToOpenAPIPaths(allCustomRoutes);
			if (prefix) {
				for (const pathKey of Object.keys(customPaths)) if (!customPaths[pathKey].servers) customPaths[pathKey].servers = [{ url: "/" }];
			}
			openApiSpec.paths = {
				...openApiSpec.paths,
				...customPaths
			};
		}
		return openApiSpec;
	}
	async registerOpenAPIRoute(app, config = {}, { prefix }) {
		const { title = "Mastra API", version = "1.0.0", description = "Mastra Server API", path = "/openapi.json" } = config;
		const openApiSpec = this.buildOpenAPISpec({
			title,
			version,
			description
		}, prefix);
		const openApiRoute = {
			method: "GET",
			path,
			responseType: "json",
			handler: async () => openApiSpec
		};
		await this.registerRoute(app, openApiRoute, { prefix });
	}
	async registerRoutes() {
		for (const route of SERVER_ROUTES) await this.registerRoute(this.app, route, { prefix: this.prefix });
		if (this.openapiPath) await this.registerOpenAPIRoute(this.app, {
			title: "Mastra API",
			version: "1.0.0",
			description: "Mastra Server API",
			path: this.openapiPath
		}, { prefix: this.prefix });
	}
	async parsePathParams(route, params) {
		const pathParamSchema = route.pathParamSchema;
		if (!pathParamSchema) return params;
		return pathParamSchema.parseAsync(params);
	}
	async parseQueryParams(route, params) {
		const queryParamSchema = route.queryParamSchema;
		if (!queryParamSchema) return params;
		const normalizedParams = parseComplexQueryParams(queryParamSchema, params);
		return queryParamSchema.parseAsync(normalizedParams);
	}
	async parseBody(route, body) {
		const bodySchema = route.bodySchema;
		if (!bodySchema) return body;
		if (body === void 0) {
			const omitted = await bodySchema.safeParseAsync(void 0);
			if (omitted.success) return omitted.data;
			const schemaType = getSchemaTypeName(unwrapOptionalNullable(bodySchema));
			if (schemaType === "object" || schemaType === "ZodObject") {
				const emptyObject = await bodySchema.safeParseAsync({});
				if (emptyObject.success) return emptyObject.data;
			}
			throw omitted.error;
		}
		return bodySchema.parseAsync(body);
	}
	static CONTEXT_LABELS = {
		query: "query parameters",
		body: "request body",
		path: "path parameters"
	};
	resolveValidationError(route, error, context) {
		const hook = route.onValidationError ?? this.mastra.getServer()?.onValidationError;
		if (hook) try {
			const result = hook(error, context);
			if (result) return result;
		} catch (hookError) {
			this.mastra.getLogger()?.error("Error in custom onValidationError hook", { error: hookError instanceof Error ? {
				message: hookError.message,
				stack: hookError.stack
			} : hookError });
		}
		return {
			status: 400,
			body: formatZodError(error, MastraServer.CONTEXT_LABELS[context])
		};
	}
};
/**
* Check FGA authorization for an HTTP route.
* Returns null if authorized or FGA not configured, or an error object if denied.
*/
async function checkRouteFGA(mastra, route, requestContext, params) {
	const fgaProvider = getFGAProvider(mastra, requestContext);
	if (!fgaProvider) return null;
	const fgaConfig = await resolveRouteFGAConfig(fgaProvider, route, requestContext, params);
	if (!fgaConfig) {
		if (fgaProvider.requireForProtectedRoutes && isProtectedFGARoute(route)) return {
			status: 403,
			error: "Forbidden",
			message: "FGA authorization denied: route FGA metadata is required"
		};
		return null;
	}
	const user = requestContext?.get("user");
	if (!user) return {
		status: 403,
		error: "Forbidden",
		message: "FGA authorization denied: authenticated user is required"
	};
	const resourceId = typeof fgaConfig.resourceId === "function" ? fgaConfig.resourceId(params, { requestContext }) : fgaConfig.resourceId || (fgaConfig.resourceIdParam ? params[fgaConfig.resourceIdParam] : void 0);
	if (!fgaConfig.resourceType || !resourceId) return {
		status: 403,
		error: "Forbidden",
		message: "FGA authorization denied: route FGA metadata is incomplete"
	};
	const effectivePermission = route.path ? getEffectivePermission(route) : null;
	const permission = fgaConfig.permission || effectivePermission || `${getFGAResourcePermissionSlug(fgaConfig.resourceType)}:${deriveFGAAction(route.method)}`;
	if (!await fgaProvider.check(user, {
		resource: {
			type: fgaConfig.resourceType,
			id: resourceId
		},
		permission,
		context: {
			resourceId,
			requestContext
		}
	})) return {
		status: 403,
		error: "Forbidden",
		message: `FGA authorization denied: cannot ${permission} on ${fgaConfig.resourceType}:${resourceId}`
	};
	return null;
}
function deriveFGAAction(method) {
	switch (method.toUpperCase()) {
		case "GET": return "read";
		case "DELETE": return "delete";
		case "POST":
		case "PUT":
		case "PATCH": return "write";
		default: return "read";
	}
}
function getFGAResourcePermissionSlug(resourceType) {
	return {
		agent: "agents",
		workflow: "workflows",
		tool: "tools",
		thread: "memory"
	}[resourceType] ?? resourceType;
}

//#region src/middleware/body-limit/index.ts
const ERROR_MESSAGE = "Payload Too Large";
/**
* Body Limit Middleware for Hono.
*
* @see {@link https://hono.dev/docs/middleware/builtin/body-limit}
*
* @param {BodyLimitOptions} options - The options for the body limit middleware.
* @param {number} options.maxSize - The maximum body size allowed.
* @param {OnError} [options.onError] - The error handler to be invoked if the specified body size is exceeded.
* @returns {MiddlewareHandler} The middleware handler function.
*
* @example
* ```ts
* const app = new Hono()
*
* app.post(
*   '/upload',
*   bodyLimit({
*     maxSize: 50 * 1024, // 50kb
*     onError: (c) => {
*       return c.text('overflow :(', 413)
*     },
*   }),
*   async (c) => {
*     const body = await c.req.parseBody()
*     if (body['file'] instanceof File) {
*       console.log(`Got file sized: ${body['file'].size}`)
*     }
*     return c.text('pass :)')
*   }
* )
* ```
*/
const bodyLimit = (options) => {
	const onError = options.onError || (() => {
		const res = new Response(ERROR_MESSAGE, { status: 413 });
		throw new HTTPException(413, { res });
	});
	const maxSize = options.maxSize;
	return async function bodyLimit(c, next) {
		if (!c.req.raw.body) return next();
		const hasTransferEncoding = c.req.raw.headers.has("transfer-encoding");
		if (c.req.raw.headers.has("content-length") && !hasTransferEncoding) return parseInt(c.req.raw.headers.get("content-length") || "0", 10) > maxSize ? onError(c) : next();
		let size = 0;
		const chunks = [];
		const rawReader = c.req.raw.body.getReader();
		for (;;) {
			const { done, value } = await rawReader.read();
			if (done) break;
			size += value.length;
			if (size > maxSize) return onError(c);
			chunks.push(value);
		}
		const requestInit = {
			body: new ReadableStream({ start(controller) {
				for (const chunk of chunks) controller.enqueue(chunk);
				controller.close();
			} }),
			duplex: "half"
		};
		c.req.raw = new Request(c.req.raw, requestInit);
		return next();
	};
};

//#region src/utils/stream.ts
/**
* @module
* Stream utility.
*/
var StreamingApi = class {
	writer;
	encoder;
	writable;
	abortSubscribers = [];
	responseReadable;
	/**
	* Whether the stream has been aborted.
	*/
	aborted = false;
	/**
	* Whether the stream has been closed normally.
	*/
	closed = false;
	constructor(writable, _readable) {
		this.writable = writable;
		this.writer = writable.getWriter();
		this.encoder = new TextEncoder();
		const reader = _readable.getReader();
		this.abortSubscribers.push(async () => {
			await reader.cancel();
		});
		this.responseReadable = new ReadableStream({
			async pull(controller) {
				const { done, value } = await reader.read();
				done ? controller.close() : controller.enqueue(value);
			},
			cancel: () => {
				if (!this.closed) this.abort();
			}
		});
	}
	async write(input) {
		try {
			if (typeof input === "string") input = this.encoder.encode(input);
			await this.writer.write(input);
		} catch {}
		return this;
	}
	async writeln(input) {
		await this.write(input + "\n");
		return this;
	}
	sleep(ms) {
		return new Promise((res) => setTimeout(res, ms));
	}
	async close() {
		this.closed = true;
		try {
			await this.writer.close();
		} catch {}
	}
	async pipe(body) {
		this.writer.releaseLock();
		try {
			await body.pipeTo(this.writable, {
				preventClose: true,
				preventAbort: true
			});
		} finally {
			this.writer = this.writable.getWriter();
		}
	}
	onAbort(listener) {
		this.abortSubscribers.push(listener);
	}
	/**
	* Abort the stream.
	* You can call this method when stream is aborted by external event.
	*/
	abort() {
		if (!this.aborted) {
			this.aborted = true;
			this.abortSubscribers.forEach((subscriber) => {
				try {
					Promise.resolve(subscriber()).catch(() => {});
				} catch {}
			});
		}
	}
};

//#region src/helper/streaming/utils.ts
let isOldBunVersion = () => {
	const version = typeof Bun !== "undefined" ? Bun.version : void 0;
	if (version === void 0) return false;
	const result = version.startsWith("1.1") || version.startsWith("1.0") || version.startsWith("0.");
	isOldBunVersion = () => result;
	return result;
};

//#region src/helper/streaming/stream.ts
const contextStash = /* @__PURE__ */ new WeakMap();
const stream = (c, cb, onError) => {
	const { readable, writable } = new TransformStream();
	const stream = new StreamingApi(writable, readable);
	if (isOldBunVersion()) c.req.raw.signal.addEventListener("abort", () => {
		if (!stream.closed) stream.abort();
	});
	contextStash.set(stream.responseReadable, c);
	(async () => {
		try {
			await cb(stream);
		} catch (e) {
			if (e === void 0) ; else if (e instanceof Error && onError) await onError(e, stream);
			else console.error(e);
		} finally {
			stream.close();
		}
	})();
	return c.newResponse(stream.responseReadable);
};

//#region src/server/browser-stream/viewer-registry.ts
/**
* ViewerRegistry manages WebSocket connections per agent and controls screencast lifecycle.
*
* Key responsibilities:
* - Track connected viewers per agentId
* - Start screencast when browser becomes active (not on viewer connect)
* - Stop screencast when last viewer disconnects
* - Broadcast frames to all viewers for an agent
*
* The browser is NOT launched when viewers connect - it only starts streaming
* when the browser is already running from agent tool usage.
*
* @example
* ```typescript
* const registry = new ViewerRegistry();
*
* // When a viewer connects
* registry.addViewer('agent-123', ws, getToolset);
*
* // When a viewer disconnects
* registry.removeViewer('agent-123', ws);
* ```
*/
var ViewerRegistry = class {
	/** Map of agentId to set of connected WebSocket contexts */
	viewers = /* @__PURE__ */ new Map();
	/** Map of agentId to active screencast stream */
	screencasts = /* @__PURE__ */ new Map();
	/** Set of viewerKeys currently starting a screencast (to prevent race conditions) */
	startingScreencasts = /* @__PURE__ */ new Set();
	/** Map of agentId to cleanup function for onBrowserReady callback */
	browserReadyCleanups = /* @__PURE__ */ new Map();
	/** Map of agentId to cleanup function for onBrowserClosed callback */
	browserClosedCleanups = /* @__PURE__ */ new Map();
	/** Map of agentId to last known URL (for dedup) */
	lastUrls = /* @__PURE__ */ new Map();
	/** Map of agentId to last known viewport dimensions (for change detection) */
	lastViewports = /* @__PURE__ */ new Map();
	/** Map of viewerKey to last broadcast status (for replay to new viewers) */
	lastStatuses = /* @__PURE__ */ new Map();
	/**
	* Add a viewer for an agent. Starts screencast if this is the first viewer.
	*
	* @param viewerKey - The viewer key (agentId or agentId:threadId for thread-scoped)
	* @param ws - The WebSocket context for this viewer
	* @param getToolset - Function to retrieve the BrowserToolset for this agent
	* @param agentId - The actual agent ID for toolset lookup (optional, defaults to viewerKey)
	* @param threadId - The thread ID for thread-scoped screencasts (optional)
	*/
	async addViewer(viewerKey, ws, getToolset, agentId, threadId) {
		let viewerSet = this.viewers.get(viewerKey);
		if (!viewerSet) {
			viewerSet = /* @__PURE__ */ new Set();
			this.viewers.set(viewerKey, viewerSet);
		}
		const wasEmpty = viewerSet.size === 0;
		viewerSet.add(ws);
		if (wasEmpty || !this.browserReadyCleanups.has(viewerKey)) await this.startScreencast(viewerKey, getToolset, agentId ?? viewerKey, threadId);
		else this.sendCurrentState(viewerKey, ws);
	}
	/**
	* Send current state (URL, viewport) to a newly connected viewer.
	*/
	sendCurrentState(viewerKey, ws) {
		try {
			const lastUrl = this.lastUrls.get(viewerKey);
			if (lastUrl) ws.send(JSON.stringify({ url: lastUrl }));
			const lastViewport = this.lastViewports.get(viewerKey);
			if (lastViewport) ws.send(JSON.stringify({ viewport: lastViewport }));
			const lastStatus = this.lastStatuses.get(viewerKey);
			if (lastStatus) ws.send(JSON.stringify({ status: lastStatus }));
		} catch (error) {
			console.warn("[ViewerRegistry] Error sending current state to new viewer:", error);
		}
	}
	/**
	* Remove a viewer. Stops screencast if this was the last viewer.
	*
	* @param viewerKey - The viewer key (agentId or agentId:threadId for thread-scoped)
	* @param ws - The WebSocket context to remove
	*/
	async removeViewer(viewerKey, ws) {
		const viewerSet = this.viewers.get(viewerKey);
		if (!viewerSet) return;
		viewerSet.delete(ws);
		if (viewerSet.size === 0) {
			this.viewers.delete(viewerKey);
			this.lastUrls.delete(viewerKey);
			this.lastViewports.delete(viewerKey);
			const readyCleanup = this.browserReadyCleanups.get(viewerKey);
			if (readyCleanup) {
				readyCleanup();
				this.browserReadyCleanups.delete(viewerKey);
			}
			const closedCleanup = this.browserClosedCleanups.get(viewerKey);
			if (closedCleanup) {
				closedCleanup();
				this.browserClosedCleanups.delete(viewerKey);
			}
			await this.stopScreencast(viewerKey);
		}
	}
	/**
	* Broadcast a binary frame to all viewers.
	*
	* @param viewerKey - The viewer key (agentId or agentId:threadId for thread-scoped)
	* @param data - The binary frame data (base64 encoded)
	*/
	broadcastFrame(viewerKey, data) {
		const viewerSet = this.viewers.get(viewerKey);
		if (!viewerSet) return;
		for (const ws of viewerSet) try {
			ws.send(data);
		} catch (error) {
			console.warn("[ViewerRegistry] Error broadcasting frame:", error);
		}
	}
	/**
	* Broadcast a status message to all viewers.
	*
	* @param viewerKey - The viewer key (agentId or agentId:threadId for thread-scoped)
	* @param status - The status message to send
	*/
	broadcastStatus(viewerKey, status) {
		if (status.status) this.lastStatuses.set(viewerKey, status.status);
		const viewerSet = this.viewers.get(viewerKey);
		if (!viewerSet) return;
		const message = JSON.stringify(status);
		for (const ws of viewerSet) try {
			ws.send(message);
		} catch (error) {
			console.warn("[ViewerRegistry] Error broadcasting status:", error);
		}
	}
	/**
	* Broadcast a URL update to all viewers (only if changed).
	*/
	broadcastUrlIfChanged(viewerKey, url) {
		if (!url) return;
		if (this.lastUrls.get(viewerKey) === url) return;
		this.lastUrls.set(viewerKey, url);
		const viewerSet = this.viewers.get(viewerKey);
		if (!viewerSet) return;
		const message = JSON.stringify({ url });
		for (const ws of viewerSet) try {
			ws.send(message);
		} catch (error) {
			console.warn("[ViewerRegistry] Error broadcasting URL:", error);
		}
	}
	/**
	* Broadcast viewport metadata to all viewers.
	* Only sends if dimensions have changed from last broadcast.
	* Called on stream start and on each frame to detect dimension changes.
	*/
	broadcastViewportIfChanged(viewerKey, viewport) {
		const last = this.lastViewports.get(viewerKey);
		if (last && last.width === viewport.width && last.height === viewport.height) return;
		this.lastViewports.set(viewerKey, {
			width: viewport.width,
			height: viewport.height
		});
		const viewerSet = this.viewers.get(viewerKey);
		if (!viewerSet) return;
		const message = { viewport: {
			width: viewport.width,
			height: viewport.height
		} };
		const json = JSON.stringify(message);
		for (const ws of viewerSet) try {
			ws.send(json);
		} catch (error) {
			console.warn("[ViewerRegistry] Error broadcasting viewport:", error);
		}
	}
	/**
	* Start screencast for a viewer. Only starts if browser is already running.
	* If browser not running, registers a callback to start when browser becomes ready.
	* If no toolset can be resolved, nothing is registered; `addViewer` retries the
	* lookup the next time a viewer connects for this key.
	*
	* @param viewerKey - The viewer key (for registry keying)
	* @param getToolset - Function to retrieve the BrowserToolset
	* @param agentId - The actual agent ID for toolset lookup
	* @param threadId - The thread ID for thread-scoped page selection (optional)
	*/
	async startScreencast(viewerKey, getToolset, agentId, threadId) {
		const toolset = await getToolset(agentId);
		if (!this.viewers.has(viewerKey)) return;
		if (!toolset) {
			console.info(`[ViewerRegistry] No browser toolset for ${viewerKey}; will retry on next viewer connect`);
			return;
		}
		if (!this.browserReadyCleanups.has(viewerKey)) {
			const cleanup = toolset.onBrowserReady(() => {
				if (!this.viewers.has(viewerKey)) return;
				const existingStream = this.screencasts.get(viewerKey);
				if (existingStream) {
					console.info(`[ViewerRegistry] Stopping old screencast for ${viewerKey} before reconnecting...`);
					this.screencasts.delete(viewerKey);
					existingStream.stop().catch(() => {});
				}
				console.info(`[ViewerRegistry] Browser ready for ${viewerKey}, starting screencast...`);
				this.doStartScreencast(viewerKey, toolset, threadId).catch((error) => {
					console.error(`[ViewerRegistry] Failed to start screencast on browser ready for ${viewerKey}:`, error);
				});
			}, threadId);
			this.browserReadyCleanups.set(viewerKey, cleanup);
		}
		if (!this.browserClosedCleanups.has(viewerKey)) {
			const cleanup = toolset.onBrowserClosed(() => {
				console.info(`[ViewerRegistry] Browser closed for ${viewerKey}, notifying viewers...`);
				this.screencasts.delete(viewerKey);
				this.broadcastStatus(viewerKey, { status: "browser_closed" });
			}, threadId);
			this.browserClosedCleanups.set(viewerKey, cleanup);
		}
		if (toolset.isBrowserRunning(threadId)) await this.doStartScreencast(viewerKey, toolset, threadId);
		else console.info(`[ViewerRegistry] Browser not running for ${viewerKey}, waiting for browser to start...`);
	}
	/**
	* Internal method to actually start the screencast stream.
	*
	* @param viewerKey - The viewer key (for registry keying and logging)
	* @param toolset - The browser toolset
	* @param threadId - The thread ID for thread-scoped page selection (optional)
	*/
	async doStartScreencast(viewerKey, toolset, threadId) {
		if (this.screencasts.has(viewerKey) || this.startingScreencasts.has(viewerKey)) return;
		this.startingScreencasts.add(viewerKey);
		try {
			this.broadcastStatus(viewerKey, { status: "browser_starting" });
			const stream = await toolset.startScreencastIfBrowserActive(threadId ? { threadId } : void 0);
			if (!stream) {
				console.warn(`[ViewerRegistry] No browser session for ${viewerKey}`);
				this.broadcastStatus(viewerKey, { status: "browser_closed" });
				return;
			}
			this.screencasts.set(viewerKey, stream);
			const currentStream = stream;
			stream.on("frame", (frame) => {
				if (this.screencasts.get(viewerKey) !== currentStream) return;
				this.broadcastFrame(viewerKey, frame.data);
				this.broadcastViewportIfChanged(viewerKey, frame.viewport);
			});
			stream.on("url", (url) => {
				if (this.screencasts.get(viewerKey) !== currentStream) return;
				this.broadcastUrlIfChanged(viewerKey, url);
			});
			stream.on("stop", (reason) => {
				if (this.screencasts.get(viewerKey) !== currentStream) {
					console.info(`[ViewerRegistry] Ignoring stop from superseded stream for ${viewerKey}`);
					return;
				}
				console.info(`[ViewerRegistry] Screencast stopped for ${viewerKey}: ${reason}`);
				this.screencasts.delete(viewerKey);
				this.broadcastStatus(viewerKey, { status: "browser_closed" });
			});
			stream.on("error", (error) => {
				if (this.screencasts.get(viewerKey) !== currentStream) return;
				console.error(`[ViewerRegistry] Screencast error for ${viewerKey}:`, error);
				this.screencasts.delete(viewerKey);
				currentStream.stop().catch((stopError) => {
					console.warn(`[ViewerRegistry] Error stopping errored screencast for ${viewerKey}:`, stopError);
				});
				this.broadcastStatus(viewerKey, { status: "browser_closed" });
			});
			this.broadcastStatus(viewerKey, { status: "streaming" });
			const initialUrl = await toolset.getCurrentUrl(threadId);
			this.broadcastUrlIfChanged(viewerKey, initialUrl);
		} catch (error) {
			console.error(`[ViewerRegistry] Failed to start screencast for ${viewerKey}:`, error);
		} finally {
			this.startingScreencasts.delete(viewerKey);
		}
	}
	/**
	* Stop screencast for an agent. Called when last viewer disconnects.
	*/
	async stopScreencast(agentId) {
		const stream = this.screencasts.get(agentId);
		if (!stream) return;
		try {
			await stream.stop();
		} catch (error) {
			console.warn(`[ViewerRegistry] Error stopping screencast for ${agentId}:`, error);
		} finally {
			this.screencasts.delete(agentId);
		}
	}
	/**
	* Get the number of viewers for an agent.
	*
	* @param agentId - The agent ID
	* @returns The number of connected viewers
	*/
	getViewerCount(agentId) {
		return this.viewers.get(agentId)?.size ?? 0;
	}
	/**
	* Check if an agent has an active screencast.
	*
	* @param agentId - The agent ID
	* @returns True if screencast is active
	*/
	hasActiveScreencast(agentId) {
		return this.screencasts.has(agentId);
	}
	/**
	* Close the browser session for an agent.
	* Stops screencast and broadcasts browser_closed status.
	* Call this before calling toolset.close() to ensure UI is notified.
	*
	* @param viewerKey - The viewer key (agentId or agentId:threadId for thread-scoped)
	*/
	async closeBrowserSession(viewerKey) {
		this.lastUrls.delete(viewerKey);
		this.lastViewports.delete(viewerKey);
		this.lastStatuses.delete(viewerKey);
		const stream = this.screencasts.get(viewerKey);
		if (stream) try {
			await stream.stop();
		} catch (error) {
			console.warn(`[ViewerRegistry] Error stopping screencast for ${viewerKey}:`, error);
			this.screencasts.delete(viewerKey);
			this.broadcastStatus(viewerKey, { status: "browser_closed" });
		}
		else this.broadcastStatus(viewerKey, { status: "browser_closed" });
	}
};
//#endregion
//#region src/server/browser-stream/input-handler.ts
const VALID_MOUSE_EVENTS = /* @__PURE__ */ new Set([
	"mousePressed",
	"mouseReleased",
	"mouseMoved",
	"mouseWheel"
]);
const VALID_KEYBOARD_EVENTS = /* @__PURE__ */ new Set([
	"keyDown",
	"keyUp",
	"char"
]);
const inputQueues = /* @__PURE__ */ new Map();
/**
* Serialize input dispatch to maintain event ordering.
* Input events must be processed in order to avoid race conditions.
*/
function enqueueInput(key, fn) {
	const next = (inputQueues.get(key) ?? Promise.resolve()).then(fn).catch(() => {});
	inputQueues.set(key, next);
}
/**
* Map of key names to Windows virtual key codes.
* Required for non-printable keys (Enter, Tab, Arrow keys, etc.)
* See: https://docs.microsoft.com/en-us/windows/win32/inputdev/virtual-key-codes
*/
const VIRTUAL_KEY_CODES = {
	Backspace: 8,
	Tab: 9,
	Enter: 13,
	Shift: 16,
	Control: 17,
	Alt: 18,
	Pause: 19,
	CapsLock: 20,
	Escape: 27,
	Space: 32,
	" ": 32,
	PageUp: 33,
	PageDown: 34,
	End: 35,
	Home: 36,
	ArrowLeft: 37,
	ArrowUp: 38,
	ArrowRight: 39,
	ArrowDown: 40,
	Insert: 45,
	Delete: 46,
	F1: 112,
	F2: 113,
	F3: 114,
	F4: 115,
	F5: 116,
	F6: 117,
	F7: 118,
	F8: 119,
	F9: 120,
	F10: 121,
	F11: 122,
	F12: 123
};
/**
* Get the Windows virtual key code for a key.
* For printable characters, uses the character code.
* For special keys, looks up in the mapping.
*/
function getVirtualKeyCode(key) {
	if (!key) return void 0;
	if (VIRTUAL_KEY_CODES[key] !== void 0) return VIRTUAL_KEY_CODES[key];
	if (key.length === 1) return key.toUpperCase().charCodeAt(0);
}
/**
* Handle an incoming WebSocket message by parsing, validating,
* and routing to the appropriate toolset injection method.
*
* Fire-and-forget: no acknowledgment sent back to client.
* Silently ignores malformed or unrecognized messages.
*
* @param data - Raw string data from WebSocket message
* @param getToolset - Function to retrieve MastraBrowser for an agent
* @param agentId - The agent ID this WebSocket connection is for
* @param threadId - The thread ID for thread-scoped operations (optional)
*/
async function handleInputMessage(data, getToolset, agentId, threadId) {
	let message;
	try {
		message = JSON.parse(data);
	} catch {
		return;
	}
	if (!isValidInputMessage(message)) return;
	const toolset = await getToolset(agentId);
	if (!toolset) return;
	const queueKey = `${agentId}:${threadId ?? "default"}`;
	switch (message.type) {
		case "mouse":
			enqueueInput(queueKey, async () => {
				try {
					await injectMouse(toolset, message, threadId);
				} catch (err) {
					if (isDisconnectionError(err)) notifyBrowserClosed(toolset, threadId);
					else if (!isExpectedInjectionError(err)) console.warn("[InputHandler] Mouse injection error:", err);
				}
			});
			break;
		case "keyboard":
			enqueueInput(queueKey, async () => {
				try {
					await injectKeyboard(toolset, message, threadId);
				} catch (err) {
					if (isDisconnectionError(err)) notifyBrowserClosed(toolset, threadId);
					else if (!isExpectedInjectionError(err)) console.warn("[InputHandler] Keyboard injection error:", err);
				}
			});
			break;
	}
}
/**
* Check if an error indicates browser disconnection (target closed).
* These errors mean the browser was externally closed.
*/
function isDisconnectionError(err) {
	if (!(err instanceof Error)) return false;
	const msg = err.message.toLowerCase();
	return msg.includes("target closed") || msg.includes("browser has been closed") || msg.includes("page has been closed") || msg.includes("session closed") || msg.includes("browser has disconnected");
}
/**
* Check if an injection error is expected (browser not ready yet).
* These are silently ignored to avoid log spam.
*/
function isExpectedInjectionError(err) {
	if (!(err instanceof Error)) return false;
	const msg = err.message.toLowerCase();
	return msg.includes("no cdp session") || msg.includes("browser not launched") || msg.includes("not connected to browser") || msg.includes("no active target") || msg.includes("target") && msg.includes("not attached");
}
/**
* Notify the browser that it was closed externally.
* This triggers the onBrowserClosed callbacks to update the UI.
* If threadId is provided and the browser supports thread-specific closing,
* only that thread's session is closed.
*/
function notifyBrowserClosed(toolset, threadId) {
	const browser = toolset;
	if (threadId && typeof browser.closeThreadSession === "function") {
		browser.closeThreadSession(threadId).catch(() => {
			browser.handleBrowserDisconnected?.();
		});
		return;
	}
	browser.handleBrowserDisconnected?.();
}
/**
* Inject a mouse event into the browser.
*/
async function injectMouse(toolset, msg, threadId) {
	await toolset.injectMouseEvent({
		type: msg.eventType,
		x: msg.x,
		y: msg.y,
		button: msg.button,
		clickCount: msg.clickCount,
		deltaX: msg.deltaX,
		deltaY: msg.deltaY,
		modifiers: msg.modifiers
	}, threadId);
}
/**
* Inject a keyboard event into the browser.
*/
async function injectKeyboard(toolset, msg, threadId) {
	const windowsVirtualKeyCode = getVirtualKeyCode(msg.key);
	await toolset.injectKeyboardEvent({
		type: msg.eventType,
		key: msg.key,
		code: msg.code,
		text: msg.text,
		modifiers: msg.modifiers,
		windowsVirtualKeyCode
	}, threadId);
}
/**
* Type guard to validate incoming messages.
* Validates structure and rejects invalid event types at the boundary.
*/
function isValidInputMessage(msg) {
	if (typeof msg !== "object" || msg === null) return false;
	const typed = msg;
	if (typed.type === "mouse") {
		if (typeof typed.eventType !== "string" || !VALID_MOUSE_EVENTS.has(typed.eventType)) return false;
		if (typeof typed.x !== "number" || typeof typed.y !== "number") return false;
		if (typed.button !== void 0 && typeof typed.button !== "string") return false;
		if (typed.deltaX !== void 0 && typeof typed.deltaX !== "number") return false;
		if (typed.deltaY !== void 0 && typeof typed.deltaY !== "number") return false;
		return true;
	}
	if (typed.type === "keyboard") {
		if (typeof typed.eventType !== "string" || !VALID_KEYBOARD_EVENTS.has(typed.eventType)) return false;
		const hasKey = typeof typed.key === "string";
		const hasCode = typeof typed.code === "string";
		const hasText = typeof typed.text === "string";
		if (!hasKey && !hasCode && !hasText) return false;
		if (typed.modifiers !== void 0 && typeof typed.modifiers !== "number") return false;
		return true;
	}
	return false;
}

//#region src/utils/accept.ts
const isWhitespace = (char) => char === 32 || char === 9 || char === 10 || char === 13;
const consumeWhitespace = (acceptHeader, startIndex) => {
	while (startIndex < acceptHeader.length) {
		if (!isWhitespace(acceptHeader.charCodeAt(startIndex))) break;
		startIndex++;
	}
	return startIndex;
};
const ignoreTrailingWhitespace = (acceptHeader, startIndex) => {
	while (startIndex > 0) {
		if (!isWhitespace(acceptHeader.charCodeAt(startIndex - 1))) break;
		startIndex--;
	}
	return startIndex;
};
const skipInvalidParam = (acceptHeader, startIndex) => {
	while (startIndex < acceptHeader.length) {
		const char = acceptHeader.charCodeAt(startIndex);
		if (char === 59) return [startIndex + 1, true];
		if (char === 44) return [startIndex + 1, false];
		startIndex++;
	}
	return [startIndex, false];
};
const skipInvalidAcceptValue = (acceptHeader, startIndex) => {
	let i = startIndex;
	let inQuotes = false;
	while (i < acceptHeader.length) {
		const char = acceptHeader.charCodeAt(i);
		if (inQuotes && char === 92) i++;
		else if (char === 34) inQuotes = !inQuotes;
		else if (!inQuotes && char === 44) return i + 1;
		i++;
	}
	return i;
};
const getNextParam = (acceptHeader, startIndex) => {
	startIndex = consumeWhitespace(acceptHeader, startIndex);
	let i = startIndex;
	let key;
	let value;
	let hasNext = false;
	while (i < acceptHeader.length) {
		const char = acceptHeader.charCodeAt(i);
		if (char === 61) {
			key = acceptHeader.slice(startIndex, ignoreTrailingWhitespace(acceptHeader, i));
			i++;
			break;
		}
		if (char === 59) return [
			i + 1,
			void 0,
			void 0,
			true
		];
		if (char === 44) return [
			i + 1,
			void 0,
			void 0,
			false
		];
		i++;
	}
	if (key === void 0) return [
		i,
		void 0,
		void 0,
		false
	];
	i = consumeWhitespace(acceptHeader, i);
	if (acceptHeader.charCodeAt(i) === 61) {
		const skipResult = skipInvalidParam(acceptHeader, i + 1);
		return [
			skipResult[0],
			key,
			void 0,
			skipResult[1]
		];
	}
	let inQuotes = false;
	const paramStartIndex = i;
	while (i < acceptHeader.length) {
		const char = acceptHeader.charCodeAt(i);
		if (inQuotes && char === 92) i++;
		else if (char === 34) {
			if (inQuotes) {
				let nextIndex = consumeWhitespace(acceptHeader, i + 1);
				const nextChar = acceptHeader.charCodeAt(nextIndex);
				if (nextIndex < acceptHeader.length && !(nextChar === 59 || nextChar === 44)) {
					const skipResult = skipInvalidParam(acceptHeader, nextIndex);
					return [
						skipResult[0],
						key,
						void 0,
						skipResult[1]
					];
				}
				value = acceptHeader.slice(paramStartIndex + 1, i);
				if (value.includes("\\")) value = value.replace(/\\(.)/g, "$1");
				if (nextChar === 44) return [
					nextIndex + 1,
					key,
					value,
					false
				];
				if (nextChar === 59) {
					hasNext = true;
					nextIndex++;
				}
				i = nextIndex;
				break;
			}
			inQuotes = true;
		} else if (!inQuotes && (char === 59 || char === 44)) {
			value = acceptHeader.slice(paramStartIndex, ignoreTrailingWhitespace(acceptHeader, i));
			if (char === 59) hasNext = true;
			i++;
			break;
		}
		i++;
	}
	return [
		i,
		key,
		value ?? acceptHeader.slice(paramStartIndex, ignoreTrailingWhitespace(acceptHeader, i)),
		hasNext
	];
};
const getNextAcceptValue = (acceptHeader, startIndex) => {
	const accept = {
		type: "",
		params: Object.create(null),
		q: 1
	};
	startIndex = consumeWhitespace(acceptHeader, startIndex);
	let i = startIndex;
	while (i < acceptHeader.length) {
		const char = acceptHeader.charCodeAt(i);
		if (char === 59 || char === 44) {
			accept.type = acceptHeader.slice(startIndex, ignoreTrailingWhitespace(acceptHeader, i));
			i++;
			if (char === 44) return [i, accept.type ? accept : void 0];
			if (!accept.type) return [skipInvalidAcceptValue(acceptHeader, i), void 0];
			break;
		}
		i++;
	}
	if (!accept.type) {
		accept.type = acceptHeader.slice(startIndex, ignoreTrailingWhitespace(acceptHeader, acceptHeader.length));
		return [acceptHeader.length, accept.type ? accept : void 0];
	}
	let param;
	let value;
	let hasNext;
	while (i < acceptHeader.length) {
		[i, param, value, hasNext] = getNextParam(acceptHeader, i);
		if (param && value) accept.params[param] = value;
		if (!hasNext) break;
	}
	return [i, accept];
};
const parseAccept = (acceptHeader) => {
	if (!acceptHeader) return [];
	const values = [];
	let i = 0;
	let accept;
	let requiresSort = false;
	let lastAccept;
	while (i < acceptHeader.length) {
		[i, accept] = getNextAcceptValue(acceptHeader, i);
		if (accept) {
			accept.q = parseQuality(accept.params.q ?? accept.params.Q);
			values.push(accept);
			if (lastAccept && lastAccept.q < accept.q) requiresSort = true;
			lastAccept = accept;
		}
	}
	if (requiresSort) values.sort((a, b) => b.q - a.q);
	return values;
};
const parseQuality = (qVal) => {
	if (qVal === void 0) return 1;
	if (qVal === "") return 1;
	if (qVal === "NaN") return 0;
	const num = Number(qVal);
	if (Number.isNaN(num)) return 1;
	if (num < 0) return 0;
	if (num > 1) return 1;
	return num;
};

//#region src/utils/compress.ts
/**
* @module
* Constants for compression.
*/
/**
* Match for compressible content type.
*/
const COMPRESSIBLE_CONTENT_TYPE_REGEX$1 = /^\s*(?:text\/(?!event-stream(?:[;\s]|$))[^;\s]+|application\/(?:javascript|json|xml|xml-dtd|ecmascript|dart|msgpack|postscript|rtf|tar|toml|vnd\.dart|vnd\.ms-fontobject|vnd\.ms-opentype|vnd\.msgpack|wasm|x-httpd-php|x-javascript|x-msgpack|x-ns-proxy-autoconfig|x-sh|x-tar|x-virtualbox-hdd|x-virtualbox-ova|x-virtualbox-ovf|x-virtualbox-vbox|x-virtualbox-vdi|x-virtualbox-vhd|x-virtualbox-vmdk|x-www-form-urlencoded)|font\/(?:otf|ttf)|image\/(?:bmp|vnd\.adobe\.photoshop|vnd\.microsoft\.icon|vnd\.ms-dds|x-icon|x-ms-bmp)|message\/rfc822|model\/gltf-binary|x-shader\/x-fragment|x-shader\/x-vertex|[^;\s]+?\+(?:json|text|xml|yaml|msgpack))(?:[;\s]|$)/i;

//#region src/middleware/compress/index.ts
const ENCODING_TYPES = ["gzip", "deflate"];
const cacheControlNoTransformRegExp = /(?:^|,)\s*?no-transform\s*?(?:,|$)/i;
const selectEncoding = (header, candidates) => {
	if (header === void 0) return;
	const accepts = parseAccept(header);
	const wildcardQ = accepts.find((a) => a.type === "*")?.q;
	let best;
	for (const enc of candidates) {
		const explicit = accepts.find((a) => a.type.toLowerCase() === enc);
		const q = explicit ? explicit.q : wildcardQ ?? 0;
		if (q === 1) return enc;
		else if (q > 0 && (!best || q > best.q)) best = {
			encoding: enc,
			q
		};
	}
	return best?.encoding;
};
const varyAcceptEncodingRegExp = /(?:^|,)\s*accept-encoding\s*(?:,|$)/i;
/**
* Compress Middleware for Hono.
*
* @see {@link https://hono.dev/docs/middleware/builtin/compress}
*
* @param {CompressionOptions} [options] - The options for the compress middleware.
* @param {'gzip' | 'deflate'} [options.encoding] - The compression scheme to allow for response compression. Either 'gzip' or 'deflate'. If not defined, both are allowed and will be used based on the Accept-Encoding header. 'gzip' is prioritized if this option is not provided and the client provides both in the Accept-Encoding header.
* @param {number} [options.threshold=1024] - The minimum size in bytes to compress. Defaults to 1024 bytes.
* @param {RegExp | Function} [options.contentTypeFilter=COMPRESSIBLE_CONTENT_TYPE_REGEX] - A RegExp or function to determine if the response Content-Type should be compressed.
* @returns {MiddlewareHandler} The middleware handler function.
*
* @example
* ```ts
* const app = new Hono()
*
* app.use(compress())
*
* // Compress only JSON responses
* app.use(compress({ contentTypeFilter: /^application\/json/ }))
*
* // Compress based on custom Content-Type logic
* app.use(compress({ contentTypeFilter: (type) => COMPRESSIBLE_CONTENT_TYPE_REGEX.test(type) || type === "application/x-myformat" }))
* ```
*/
const compress = (options) => {
	const threshold = 1024;
	const candidates = ENCODING_TYPES;
	const contentTypeFilter = COMPRESSIBLE_CONTENT_TYPE_REGEX$1;
	const shouldCompress = typeof contentTypeFilter === "function" ? (res) => {
		const type = res.headers.get("Content-Type");
		return type && contentTypeFilter(type);
	} : (res) => {
		const type = res.headers.get("Content-Type");
		return type && contentTypeFilter.test(type);
	};
	return async function compress(ctx, next) {
		await next();
		const contentLength = ctx.res.headers.get("Content-Length");
		if (ctx.res.status === 206 || ctx.res.headers.has("Content-Encoding") || ctx.res.headers.has("Transfer-Encoding") || ctx.req.method === "HEAD" || contentLength && Number(contentLength) < threshold || !shouldCompress(ctx.res) || !shouldTransform(ctx.res)) return;
		const current = ctx.res.headers.get("Vary");
		if (current !== "*" && !(current && varyAcceptEncodingRegExp.test(current))) ctx.header("Vary", current ? `${current}, Accept-Encoding` : "Accept-Encoding");
		const accepted = ctx.req.header("Accept-Encoding");
		const encoding = selectEncoding(accepted, candidates);
		if (!encoding || !ctx.res.body) return;
		const stream = new CompressionStream(encoding);
		ctx.res = new Response(ctx.res.body.pipeThrough(stream), ctx.res);
		ctx.res.headers.delete("Content-Length");
		ctx.res.headers.set("Content-Encoding", encoding);
		const etag = ctx.res.headers.get("ETag");
		if (etag && !etag.startsWith("W/")) ctx.res.headers.set("ETag", `W/${etag}`);
	};
};
const shouldTransform = (res) => {
	const cacheControl = res.headers.get("Cache-Control");
	return !cacheControl || !cacheControlNoTransformRegExp.test(cacheControl);
};

//#region src/middleware/cors/index.ts
/**
* CORS Middleware for Hono.
*
* @see {@link https://hono.dev/docs/middleware/builtin/cors}
*
* @param {CORSOptions} [options] - The options for the CORS middleware.
* @param {string | string[] | ((origin: string, c: Context) => Promise<string | undefined | null> | string | undefined | null)} [options.origin='*'] - The value of "Access-Control-Allow-Origin" CORS header.
* @param {string[] | ((origin: string, c: Context) => Promise<string[]> | string[])} [options.allowMethods=['GET', 'HEAD', 'PUT', 'POST', 'DELETE', 'PATCH', 'QUERY']] - The value of "Access-Control-Allow-Methods" CORS header.
* @param {string[]} [options.allowHeaders=[]] - The value of "Access-Control-Allow-Headers" CORS header.
* @param {number} [options.maxAge] - The value of "Access-Control-Max-Age" CORS header.
* @param {boolean} [options.credentials] - The value of "Access-Control-Allow-Credentials" CORS header.
* @param {string[]} [options.exposeHeaders=[]] - The value of "Access-Control-Expose-Headers" CORS header.
* @returns {MiddlewareHandler} The middleware handler function.
*
* @example
* ```ts
* const app = new Hono()
*
* app.use('/api/*', cors())
* app.use(
*   '/api2/*',
*   cors({
*     origin: 'http://example.com',
*     allowHeaders: ['X-Custom-Header', 'Upgrade-Insecure-Requests'],
*     allowMethods: ['POST', 'GET', 'OPTIONS'],
*     exposeHeaders: ['Content-Length', 'X-Kuma-Revision'],
*     maxAge: 600,
*     credentials: true,
*   })
* )
*
* app.all('/api/abc', (c) => {
*   return c.json({ success: true })
* })
* app.all('/api2/abc', (c) => {
*   return c.json({ success: true })
* })
* ```
*/
const cors = (options) => {
	const opts = {
		origin: "*",
		allowMethods: [
			"GET",
			"HEAD",
			"PUT",
			"POST",
			"DELETE",
			"PATCH",
			"QUERY"
		],
		allowHeaders: [],
		exposeHeaders: [],
		...options
	};
	const exposeHeadersStr = opts.exposeHeaders?.length ? opts.exposeHeaders.join(",") : void 0;
	const allowHeadersStr = opts.allowHeaders?.length ? opts.allowHeaders.join(",") : void 0;
	const findAllowOrigin = ((optsOrigin) => {
		if (typeof optsOrigin === "string") {
			if (optsOrigin === "*") return () => optsOrigin;
			else return (origin) => optsOrigin === origin ? origin : null;
		} else if (typeof optsOrigin === "function") return optsOrigin;
		else return (origin) => optsOrigin.includes(origin) ? origin : null;
	})(opts.origin);
	const findAllowMethods = ((optsAllowMethods) => {
		if (typeof optsAllowMethods === "function") return async (origin, c) => (await optsAllowMethods(origin, c)).join(",");
		else if (Array.isArray(optsAllowMethods)) {
			const methodsStr = optsAllowMethods.join(",");
			return () => methodsStr;
		} else return () => "";
	})(opts.allowMethods);
	return async function cors(c, next) {
		function set(key, value) {
			c.res.headers.set(key, value);
		}
		const allowOrigin = await findAllowOrigin(c.req.header("origin") || "", c);
		if (allowOrigin) set("Access-Control-Allow-Origin", allowOrigin);
		if (opts.credentials) set("Access-Control-Allow-Credentials", "true");
		if (exposeHeadersStr) set("Access-Control-Expose-Headers", exposeHeadersStr);
		if (c.req.method === "OPTIONS") {
			if (opts.origin !== "*") c.res.headers.append("Vary", "Origin");
			if (opts.maxAge != null) set("Access-Control-Max-Age", opts.maxAge.toString());
			const allowMethods = await findAllowMethods(c.req.header("origin") || "", c);
			if (allowMethods) set("Access-Control-Allow-Methods", allowMethods);
			let headersStr = allowHeadersStr;
			if (!headersStr) {
				const requestHeaders = c.req.header("Access-Control-Request-Headers");
				if (requestHeaders) headersStr = requestHeaders.split(",").map((h) => h.trim()).join(",");
			}
			if (headersStr) {
				set("Access-Control-Allow-Headers", headersStr);
				c.res.headers.append("Vary", "Access-Control-Request-Headers");
			}
			c.res.headers.delete("Content-Length");
			c.res.headers.delete("Content-Type");
			return new Response(null, {
				headers: c.res.headers,
				status: 204,
				statusText: "No Content"
			});
		}
		await next();
		if (opts.origin !== "*") c.header("Vary", "Origin", { append: true });
	};
};

//#region src/utils/color.ts
/**
* @module
* Color utility.
*/
/**
* Get whether color change on terminal is enabled or disabled.
* If `NO_COLOR` environment variable is set, this function returns `false`.
* Unlike getColorEnabledAsync(), this cannot check Cloudflare environment variables.
* @see {@link https://no-color.org/}
*
* @returns {boolean}
*/
function getColorEnabled() {
	const { process, Deno } = globalThis;
	return !(typeof Deno?.noColor === "boolean" ? Deno.noColor : process !== void 0 ? "NO_COLOR" in process?.env : false);
}
/**
* Get whether color change on terminal is enabled or disabled.
* If `NO_COLOR` environment variable is set, this function returns `false`.
* @see {@link https://no-color.org/}
*
* @returns {boolean}
*/
async function getColorEnabledAsync() {
	const { navigator } = globalThis;
	const cfWorkers = "cloudflare:workers";
	return !(navigator !== void 0 && navigator.userAgent === "Cloudflare-Workers" ? await (async () => {
		try {
			return "NO_COLOR" in ((await import(cfWorkers)).env ?? {});
		} catch {
			return false;
		}
	})() : !getColorEnabled());
}

//#region src/middleware/logger/index.ts
const humanize = (times) => {
	const [delimiter, separator] = [",", "."];
	return times.map((v) => v.replace(/(\d)(?=(\d\d\d)+(?!\d))/g, "$1" + delimiter)).join(separator);
};
const time = (start) => {
	const delta = Date.now() - start;
	return humanize([delta < 1e3 ? delta + "ms" : Math.round(delta / 1e3) + "s"]);
};
const colorStatus = async (status) => {
	if (await getColorEnabledAsync()) switch (status / 100 | 0) {
		case 5: return `\x1b[31m${status}\x1b[0m`;
		case 4: return `\x1b[33m${status}\x1b[0m`;
		case 3: return `\x1b[36m${status}\x1b[0m`;
		case 2: return `\x1b[32m${status}\x1b[0m`;
	}
	return `${status}`;
};
async function log(fn, prefix, method, path, status = 0, elapsed) {
	fn(prefix === "<--" ? `${prefix} ${method} ${path}` : `${prefix} ${method} ${path} ${await colorStatus(status)} ${elapsed}`);
}
/**
* Logger Middleware for Hono.
*
* @see {@link https://hono.dev/docs/middleware/builtin/logger}
*
* @param {PrintFunc} [fn=console.log] - Optional function for customized logging behavior.
* @returns {MiddlewareHandler} The middleware handler function.
*
* @example
* ```ts
* const app = new Hono()
*
* app.use(logger())
* app.get('/', (c) => c.text('Hello Hono!'))
* ```
*/
const logger = (fn = console.log) => {
	return async function logger(c, next) {
		const { method, url } = c.req;
		const path = url.slice(url.indexOf("/", 8));
		await log(fn, "<--", method, path);
		const start = Date.now();
		await next();
		await log(fn, "-->", method, path, c.res.status, time(start));
	};
};

//#region src/middleware/timeout/index.ts
const defaultTimeoutException = new HTTPException(504, { message: "Gateway Timeout" });
/**
* Timeout Middleware for Hono.
*
* @param {number} duration - The timeout duration in milliseconds.
* @param {HTTPExceptionFunction | HTTPException} [exception=defaultTimeoutException] - The exception to throw when the timeout occurs. Can be a function that returns an HTTPException or an HTTPException object.
* @returns {MiddlewareHandler} The middleware handler function.
*
* @example
* ```ts
* const app = new Hono()
*
* app.use(
*   '/long-request',
*   timeout(5000) // Set timeout to 5 seconds
* )
*
* app.get('/long-request', async (c) => {
*   await someLongRunningFunction()
*   return c.text('Completed within time limit')
* })
* ```
*/
const timeout = (duration, exception = defaultTimeoutException) => {
	return async function timeout(context, next) {
		let timer;
		const timeoutPromise = new Promise((_, reject) => {
			timer = setTimeout(() => {
				reject(typeof exception === "function" ? exception(context) : exception);
			}, duration);
		});
		try {
			await Promise.race([next(), timeoutPromise]);
		} finally {
			if (timer !== void 0) clearTimeout(timer);
		}
	};
};

//#endregion
//#region ../../../../../setup-pnpm/node_modules/.bin/store/v11/links/@hono/node-server/2.1.3/77fb7b7d8de4d768581a422b8f5a10243114c5fe830d14a0f6e462670caab44a/node_modules/@hono/node-server/dist/index.mjs
var RequestError = class extends Error {
	constructor(message, options) {
		super(message, options);
		this.name = "RequestError";
	}
};
const nonJoinedHeaders = /* @__PURE__ */ new Set([
	"age",
	"authorization",
	"content-length",
	"content-type",
	"etag",
	"expires",
	"from",
	"host",
	"if-modified-since",
	"if-unmodified-since",
	"last-modified",
	"location",
	"max-forwards",
	"proxy-authorization",
	"referer",
	"retry-after",
	"server",
	"user-agent"
]);
const validHeaderName = /^[!#$%&'*+\-.^_`|~\dA-Za-z]+$/;
const isHttpWhitespace = (code) => code === 9 || code === 10 || code === 13 || code === 32;
const normalizeHeaderValue = (value) => {
	if (!isHttpWhitespace(value.charCodeAt(0)) && !isHttpWhitespace(value.charCodeAt(value.length - 1))) return value;
	let start = 0;
	let end = value.length;
	while (start < end && isHttpWhitespace(value.charCodeAt(start))) start++;
	while (end > start && isHttpWhitespace(value.charCodeAt(end - 1))) end--;
	return value.slice(start, end);
};
const forbiddenHeaderValue = /[\0\r\n]/;
const GlobalHeaders = globalThis.Headers;
const materializeHeaders = (rawHeaders, HeadersCtor = GlobalHeaders) => {
	const headers = new HeadersCtor();
	for (let i = 0; i < rawHeaders.length; i += 2) {
		const name = rawHeaders[i];
		if (!name.startsWith(":")) headers.append(name, rawHeaders[i + 1]);
	}
	return headers;
};
var RequestHeaders = class {
	#incoming;
	#rawHeaders;
	#headers;
	#invalidValue;
	constructor(incoming) {
		this.#incoming = incoming;
		if (incoming instanceof Http2ServerRequest) this.#rawHeaders = incoming.rawHeaders.slice();
	}
	get #lazyRawHeaders() {
		return this.#rawHeaders ??= this.#incoming.rawHeaders.slice();
	}
	get #native() {
		if (!this.#headers) {
			this.#headers = materializeHeaders(this.#lazyRawHeaders);
			this.#rawHeaders = void 0;
		}
		return this.#headers;
	}
	#normalizedName(name) {
		if (typeof name !== "string") return;
		if (!validHeaderName.test(name)) throw new TypeError(`Invalid header name: ${name}`);
		return name.toLowerCase();
	}
	#lookupHttp1(lowerName) {
		const headers = this.#incoming instanceof Http2ServerRequest ? void 0 : this.#incoming.headers;
		if (!headers || nonJoinedHeaders.has(lowerName) || lowerName === "set-cookie" || lowerName === "__proto__") return;
		if (!Object.hasOwn(headers, lowerName)) return null;
		const rawValue = headers[lowerName];
		if (typeof rawValue === "string") {
			const value = normalizeHeaderValue(rawValue);
			return forbiddenHeaderValue.test(value) ? void 0 : value;
		}
	}
	#lookup(rawHeaders, lowerName) {
		const separator = lowerName === "cookie" ? "; " : ", ";
		let value = null;
		for (let i = 0; i < rawHeaders.length; i += 2) {
			const rawName = rawHeaders[i];
			if (rawName.length === lowerName.length && rawName.toLowerCase() === lowerName) {
				const rawValue = normalizeHeaderValue(rawHeaders[i + 1]);
				if (forbiddenHeaderValue.test(rawValue)) {
					this.#invalidValue = true;
					return;
				}
				value = value === null ? rawValue : value + separator + rawValue;
			}
		}
		return value;
	}
	append(name, value) {
		this.#native.append(name, value);
	}
	delete(name) {
		this.#native.delete(name);
	}
	get(name) {
		const lowerName = this.#normalizedName(name);
		if (lowerName && !this.#headers && !this.#invalidValue) {
			const http1Value = this.#lookupHttp1(lowerName);
			if (http1Value !== void 0) return http1Value;
			const value = this.#lookup(this.#lazyRawHeaders, lowerName);
			if (value !== void 0) return value;
		}
		return this.#native.get(name);
	}
	has(name) {
		const lowerName = this.#normalizedName(name);
		if (lowerName && !this.#headers && !this.#invalidValue) {
			const http1Value = this.#lookupHttp1(lowerName);
			if (http1Value !== void 0) return http1Value !== null;
			const value = this.#lookup(this.#lazyRawHeaders, lowerName);
			if (value !== void 0) return value !== null;
		}
		return this.#native.has(name);
	}
	set(name, value) {
		this.#native.set(name, value);
	}
	getSetCookie() {
		return this.#native.getSetCookie();
	}
	keys() {
		return this.#native.keys();
	}
	values() {
		return this.#native.values();
	}
	entries() {
		return this.#native.entries();
	}
	forEach(callback, thisArg) {
		this.#native.forEach((value, key) => {
			callback.call(thisArg, value, key, this);
		});
	}
	[Symbol.iterator]() {
		return this.entries();
	}
};
Object.defineProperty(RequestHeaders.prototype, Symbol.for("nodejs.util.inspect.custom"), { value: function(depth, options, inspectFn) {
	return `Headers (lightweight) ${inspectFn(Object.fromEntries(this), {
		...options,
		depth: depth == null ? null : depth - 1
	})}`;
} });
Object.setPrototypeOf(RequestHeaders.prototype, GlobalHeaders.prototype);
const newHeadersFromIncoming = (incoming) => globalThis.Headers === GlobalHeaders ? new RequestHeaders(incoming) : materializeHeaders(incoming.rawHeaders, globalThis.Headers);
const reValidRequestUrl = /^\/[!#$&-;=?-\[\]_a-z~]*$/;
const reDotSegment = /\/\.\.?(?:[/?#]|$)/;
const reValidHost = /^[a-z0-9._-]+(?::(?:[1-5]\d{3,4}|[6-9]\d{3}))?$/;
const buildUrl = (scheme, host, incomingUrl) => {
	const url = `${scheme}://${host}${incomingUrl}`;
	if (!reValidHost.test(host)) {
		const urlObj = new URL(url);
		if (urlObj.hostname.length !== host.length && urlObj.hostname !== (host.includes(":") ? host.replace(/:\d+$/, "") : host).toLowerCase()) throw new RequestError("Invalid host header");
		return urlObj.href;
	} else if (incomingUrl.length === 0) return url + "/";
	else {
		if (incomingUrl.charCodeAt(0) !== 47) throw new RequestError("Invalid URL");
		if (!reValidRequestUrl.test(incomingUrl) || reDotSegment.test(incomingUrl)) return new URL(url).href;
		return url;
	}
};
const toRequestError = (e) => {
	if (e instanceof RequestError) return e;
	return new RequestError(e.message, { cause: e });
};
const GlobalRequest = global.Request;
var Request$1 = class extends GlobalRequest {
	constructor(input, options) {
		if (typeof input === "object" && getRequestCache in input) {
			const hasReplacementBody = options !== void 0 && "body" in options && options.body != null;
			if (input[bodyConsumedDirectlyKey] && !hasReplacementBody) throw new TypeError("Cannot construct a Request with a Request object that has already been used.");
			input = input[getRequestCache]();
		}
		if (typeof (options?.body)?.getReader !== "undefined") options.duplex ??= "half";
		super(input, options);
	}
};
const wrapBodyStream = Symbol("wrapBodyStream");
const byteExactEncodings = /* @__PURE__ */ new Set([
	"latin1",
	"binary",
	"hex",
	"base64",
	"base64url"
]);
const isByteExactEncoding = (encoding) => encoding === null || byteExactEncodings.has(encoding);
const bodyBufferedBeforeDisconnectKey = Symbol("bodyBufferedBeforeDisconnect");
const bodyBufferedLengthBeforeDisconnectKey = Symbol("bodyBufferedLengthBeforeDisconnect");
const toBufferChunk = (chunk, encoding) => Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk, encoding ?? "utf8");
const isRecoverableDisconnectedIncoming = (incoming) => !(incoming instanceof Http2ServerRequest) && !!incoming.complete && !!incoming.readableAborted && typeof incoming.read === "function" && isByteExactEncoding(incoming.readableEncoding);
const recordBodyBufferedBeforeDisconnect = (incoming) => {
	if (incoming.readableDidRead || !isRecoverableDisconnectedIncoming(incoming)) return;
	const incomingWithRecovery = incoming;
	incomingWithRecovery[bodyBufferedLengthBeforeDisconnectKey] ??= incoming.readableLength;
};
const readBodyBufferedBeforeDisconnect = (incoming, chunks) => {
	if (incoming.readableDidRead && !chunks || !isRecoverableDisconnectedIncoming(incoming)) return;
	const incomingWithRecovery = incoming;
	if (incomingWithRecovery[bodyBufferedBeforeDisconnectKey] !== void 0) return incomingWithRecovery[bodyBufferedBeforeDisconnectKey];
	let result;
	const errored = incoming.errored;
	if (errored && errored.code !== "ECONNRESET") result = errored;
	else if (incomingWithRecovery[bodyBufferedLengthBeforeDisconnectKey] !== void 0 && incoming.readableLength !== incomingWithRecovery[bodyBufferedLengthBeforeDisconnectKey]) result = newBodyUnusableError();
	else {
		const bodyChunks = chunks ?? [];
		const chunk = incoming.read();
		if (chunk !== null) bodyChunks.push(toBufferChunk(chunk, incoming.readableEncoding));
		const buffer = bodyChunks.length === 1 ? bodyChunks[0] : Buffer.concat(bodyChunks);
		result = buffer;
		const contentLength = incoming.headers["content-length"];
		if (typeof contentLength === "string" && /^\d+$/.test(contentLength)) {
			const expectedLength = Number(contentLength);
			if (Number.isSafeInteger(expectedLength) && buffer.length !== expectedLength) result = newBodyUnusableError();
		}
	}
	incomingWithRecovery[bodyBufferedBeforeDisconnectKey] = result;
	return result;
};
const enqueueBufferedBody = (controller, buffered) => {
	if (buffered instanceof Error) {
		controller.error(buffered);
		return;
	}
	if (buffered.length > 0) controller.enqueue(buffered);
	controller.close();
};
const newRequestFromIncoming = (method, url, headers, incoming, abortController) => {
	const init = {
		method,
		headers,
		signal: abortController.signal
	};
	if (method === "TRACE") {
		init.method = "GET";
		const req = new Request$1(url, init);
		Object.defineProperty(req, "method", { get() {
			return "TRACE";
		} });
		return req;
	}
	if (!(method === "GET" || method === "HEAD")) if ("rawBody" in incoming && incoming.rawBody instanceof Buffer) init.body = new ReadableStream({ start(controller) {
		controller.enqueue(incoming.rawBody);
		controller.close();
	} });
	else if (incoming[wrapBodyStream]) {
		let reader;
		init.body = new ReadableStream({ async pull(controller) {
			try {
				if (!reader) {
					const buffered = readBodyBufferedBeforeDisconnect(incoming);
					if (buffered !== void 0) {
						enqueueBufferedBody(controller, buffered);
						return;
					}
				}
				reader ||= Readable.toWeb(incoming).getReader();
				const { done, value } = await reader.read();
				if (done) controller.close();
				else controller.enqueue(value);
			} catch (error) {
				controller.error(error);
			}
		} });
	} else {
		const buffered = readBodyBufferedBeforeDisconnect(incoming);
		if (buffered !== void 0) init.body = new ReadableStream({ start(controller) {
			enqueueBufferedBody(controller, buffered);
		} });
		else init.body = Readable.toWeb(incoming);
	}
	return new Request$1(url, init);
};
const getRequestCache = Symbol("getRequestCache");
const requestCache = Symbol("requestCache");
const incomingKey = Symbol("incomingKey");
const urlKey = Symbol("urlKey");
const methodKey = Symbol("methodKey");
const headersKey = Symbol("headersKey");
const abortControllerKey = Symbol("abortControllerKey");
const getAbortController = Symbol("getAbortController");
const abortRequest = Symbol("abortRequest");
const bodyBufferKey = Symbol("bodyBuffer");
const bodyReadPromiseKey = Symbol("bodyReadPromise");
const bodyConsumedDirectlyKey = Symbol("bodyConsumedDirectly");
const bodyLockReaderKey = Symbol("bodyLockReader");
const abortReasonKey = Symbol("abortReason");
const newBodyUnusableError = () => {
	return /* @__PURE__ */ new TypeError("Body is unusable");
};
const rejectBodyUnusable = () => {
	return Promise.reject(newBodyUnusableError());
};
const textDecoder = new TextDecoder();
const consumeBodyDirectOnce = (request) => {
	if (request[bodyConsumedDirectlyKey]) return rejectBodyUnusable();
	request[bodyConsumedDirectlyKey] = true;
};
const toArrayBuffer = (buf) => {
	return buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.byteLength);
};
const contentType = (request) => {
	return (request[headersKey] ||= newHeadersFromIncoming(request[incomingKey])).get("content-type") || "";
};
const methodTokenRegExp = /^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$/;
const normalizeIncomingMethod = (method) => {
	if (typeof method !== "string" || method.length === 0) return "GET";
	switch (method) {
		case "DELETE":
		case "GET":
		case "HEAD":
		case "OPTIONS":
		case "PATCH":
		case "POST":
		case "PUT":
		case "QUERY": return method;
	}
	const upper = method.toUpperCase();
	switch (upper) {
		case "DELETE":
		case "GET":
		case "HEAD":
		case "OPTIONS":
		case "POST":
		case "PUT": return upper;
		default: return method;
	}
};
const validateDirectReadMethod = (method) => {
	if (!methodTokenRegExp.test(method)) return /* @__PURE__ */ new TypeError(`'${method}' is not a valid HTTP method.`);
	const normalized = method.toUpperCase();
	if (normalized === "CONNECT" || normalized === "TRACK" || normalized === "TRACE" && method !== "TRACE") return /* @__PURE__ */ new TypeError(`'${method}' HTTP method is unsupported.`);
};
const readBodyWithFastPath = (request, method, fromBuffer) => {
	if (request[bodyConsumedDirectlyKey]) return rejectBodyUnusable();
	const methodName = request.method;
	if (methodName === "GET" || methodName === "HEAD") return request[getRequestCache]()[method]();
	const methodValidationError = validateDirectReadMethod(methodName);
	if (methodValidationError) return Promise.reject(methodValidationError);
	if (request[requestCache]) {
		if (methodName !== "TRACE") return request[requestCache][method]();
	}
	const alreadyUsedError = consumeBodyDirectOnce(request);
	if (alreadyUsedError) return alreadyUsedError;
	const raw = readRawBodyIfAvailable(request);
	if (raw) {
		const result = Promise.resolve(fromBuffer(raw, request));
		request[bodyBufferKey] = void 0;
		return result;
	}
	return readBodyDirect(request).then((buf) => {
		const result = fromBuffer(buf, request);
		request[bodyBufferKey] = void 0;
		return result;
	});
};
const readRawBodyIfAvailable = (request) => {
	const incoming = request[incomingKey];
	if ("rawBody" in incoming && incoming.rawBody instanceof Buffer) return incoming.rawBody;
};
const normalizeAbortError = (request, incoming) => {
	if (incoming.errored) return incoming.errored;
	const reason = request[abortReasonKey];
	if (reason !== void 0) return reason instanceof Error ? reason : new Error(String(reason));
	return /* @__PURE__ */ new Error("Client connection prematurely closed.");
};
const readBodyDirect = (request) => {
	if (request[bodyBufferKey]) return Promise.resolve(request[bodyBufferKey]);
	if (request[bodyReadPromiseKey]) return request[bodyReadPromiseKey];
	const incoming = request[incomingKey];
	if (incoming.readableDidRead) return rejectBodyUnusable();
	const buffered = readBodyBufferedBeforeDisconnect(incoming);
	if (buffered !== void 0) {
		if (buffered instanceof Error) return Promise.reject(buffered);
		request[bodyBufferKey] = buffered;
		return Promise.resolve(buffered);
	}
	const promise = new Promise((resolve, reject) => {
		const chunks = [];
		let settled = false;
		const finish = (callback) => {
			if (settled) return;
			settled = true;
			cleanup();
			callback();
		};
		const recoverCompleteBodyAfterDisconnect = (error) => {
			const streamError = incoming.errored ?? error;
			if (!isRecoverableDisconnectedIncoming(incoming) || streamError && streamError.code !== "ECONNRESET") return false;
			finish(() => {
				const recovered = readBodyBufferedBeforeDisconnect(incoming, chunks);
				if (recovered instanceof Error) reject(recovered);
				else if (recovered === void 0) reject(error ?? normalizeAbortError(request, incoming));
				else {
					request[bodyBufferKey] = recovered;
					resolve(recovered);
				}
			});
			return true;
		};
		const onData = (chunk) => {
			chunks.push(toBufferChunk(chunk, incoming.readableEncoding));
		};
		const onEnd = () => {
			finish(() => {
				const buffer = chunks.length === 1 ? chunks[0] : Buffer.concat(chunks);
				request[bodyBufferKey] = buffer;
				resolve(buffer);
			});
		};
		const onError = (error) => {
			if (recoverCompleteBodyAfterDisconnect(error)) return;
			finish(() => {
				reject(error);
			});
		};
		const onClose = () => {
			if (incoming.readableEnded) {
				onEnd();
				return;
			}
			if (recoverCompleteBodyAfterDisconnect()) return;
			finish(() => {
				reject(normalizeAbortError(request, incoming));
			});
		};
		const cleanup = () => {
			incoming.off("data", onData);
			incoming.off("end", onEnd);
			incoming.off("error", onError);
			incoming.off("close", onClose);
			request[bodyReadPromiseKey] = void 0;
		};
		incoming.on("data", onData);
		incoming.on("end", onEnd);
		incoming.on("error", onError);
		incoming.on("close", onClose);
		queueMicrotask(() => {
			if (settled) return;
			if (incoming.readableEnded) onEnd();
			else if (incoming.errored) onError(incoming.errored);
			else if (incoming.destroyed) onClose();
		});
	});
	request[bodyReadPromiseKey] = promise;
	return promise;
};
const requestPrototype = {
	get method() {
		return this[methodKey];
	},
	get url() {
		return this[urlKey];
	},
	get headers() {
		return this[headersKey] ||= newHeadersFromIncoming(this[incomingKey]);
	},
	[abortRequest](reason) {
		if (this[abortReasonKey] === void 0) this[abortReasonKey] = reason;
		const abortController = this[abortControllerKey];
		if (abortController && !abortController.signal.aborted) abortController.abort(reason);
	},
	[getAbortController]() {
		this[abortControllerKey] ||= new AbortController();
		if (this[abortReasonKey] !== void 0 && !this[abortControllerKey].signal.aborted) this[abortControllerKey].abort(this[abortReasonKey]);
		return this[abortControllerKey];
	},
	[getRequestCache]() {
		const abortController = this[getAbortController]();
		if (this[requestCache]) return this[requestCache];
		const method = this.method;
		if (this[bodyConsumedDirectlyKey] && !(method === "GET" || method === "HEAD")) {
			this[bodyBufferKey] = void 0;
			const init = {
				method: method === "TRACE" ? "GET" : method,
				headers: this.headers,
				signal: abortController.signal
			};
			if (method !== "TRACE") {
				init.body = new ReadableStream({ start(c) {
					c.close();
				} });
				init.duplex = "half";
			}
			const req = new Request$1(this[urlKey], init);
			if (method === "TRACE") Object.defineProperty(req, "method", { get() {
				return "TRACE";
			} });
			return this[requestCache] = req;
		}
		return this[requestCache] = newRequestFromIncoming(this.method, this[urlKey], this.headers, this[incomingKey], abortController);
	},
	get body() {
		if (!this[bodyConsumedDirectlyKey]) return this[getRequestCache]().body;
		const request = this[getRequestCache]();
		if (!this[bodyLockReaderKey] && request.body) this[bodyLockReaderKey] = request.body.getReader();
		return request.body;
	},
	get bodyUsed() {
		if (this[bodyConsumedDirectlyKey]) return true;
		if (this[requestCache]) return this[requestCache].bodyUsed;
		return false;
	}
};
Object.defineProperty(requestPrototype, "signal", { get() {
	return this[getAbortController]().signal;
} });
[
	"cache",
	"credentials",
	"destination",
	"integrity",
	"mode",
	"redirect",
	"referrer",
	"referrerPolicy",
	"keepalive"
].forEach((k) => {
	Object.defineProperty(requestPrototype, k, { get() {
		return this[getRequestCache]()[k];
	} });
});
["clone", "formData"].forEach((k) => {
	Object.defineProperty(requestPrototype, k, { value: function() {
		if (this[bodyConsumedDirectlyKey]) {
			if (k === "clone") throw newBodyUnusableError();
			return rejectBodyUnusable();
		}
		return this[getRequestCache]()[k]();
	} });
});
Object.defineProperty(requestPrototype, "text", { value: function() {
	return readBodyWithFastPath(this, "text", (buf) => textDecoder.decode(buf));
} });
Object.defineProperty(requestPrototype, "arrayBuffer", { value: function() {
	return readBodyWithFastPath(this, "arrayBuffer", (buf) => toArrayBuffer(buf));
} });
Object.defineProperty(requestPrototype, "blob", { value: function() {
	return readBodyWithFastPath(this, "blob", (buf, request) => {
		const type = contentType(request);
		return new Response(buf, type ? { headers: { "content-type": type } } : void 0).blob();
	});
} });
Object.defineProperty(requestPrototype, "json", { value: function() {
	if (this[bodyConsumedDirectlyKey]) return rejectBodyUnusable();
	return this.text().then(JSON.parse);
} });
Object.defineProperty(requestPrototype, Symbol.for("nodejs.util.inspect.custom"), { value: function(depth, options, inspectFn) {
	return `Request (lightweight) ${inspectFn({
		method: this.method,
		url: this.url,
		headers: this.headers,
		nativeRequest: this[requestCache]
	}, {
		...options,
		depth: depth == null ? null : depth - 1
	})}`;
} });
Object.setPrototypeOf(requestPrototype, Request$1.prototype);
const newRequest = (incoming, defaultHostname) => {
	const req = Object.create(requestPrototype);
	req[incomingKey] = incoming;
	req[methodKey] = normalizeIncomingMethod(incoming.method);
	const incomingUrl = incoming.url || "";
	if (incomingUrl[0] !== "/" && (incomingUrl.startsWith("http://") || incomingUrl.startsWith("https://"))) {
		if (incoming instanceof Http2ServerRequest) throw new RequestError("Absolute URL for :path is not allowed in HTTP/2");
		try {
			req[urlKey] = new URL(incomingUrl).href;
		} catch (e) {
			throw new RequestError("Invalid absolute URL", { cause: e });
		}
		return req;
	}
	const host = (incoming instanceof Http2ServerRequest ? incoming.authority : incoming.headers.host) || defaultHostname;
	if (!host) throw new RequestError("Missing host header");
	let scheme;
	if (incoming instanceof Http2ServerRequest) {
		scheme = incoming.scheme;
		if (!(scheme === "http" || scheme === "https")) throw new RequestError("Unsupported scheme");
	} else scheme = incoming.socket && incoming.socket.encrypted ? "https" : "http";
	try {
		req[urlKey] = buildUrl(scheme, host, incomingUrl);
	} catch (e) {
		if (e instanceof RequestError) throw e;
		else throw new RequestError("Invalid URL", { cause: e });
	}
	return req;
};
const defaultContentType = "text/plain; charset=UTF-8";
const responseCache = Symbol("responseCache");
const getResponseCache = Symbol("getResponseCache");
const cacheKey = Symbol("cache");
const GlobalResponse = global.Response;
var Response$1 = class Response$1 {
	#body;
	#init;
	[getResponseCache]() {
		const cache = this[cacheKey];
		const liveHeaders = cache && cache[2] instanceof Headers ? cache[2] : void 0;
		delete this[cacheKey];
		return this[responseCache] ||= new GlobalResponse(this.#body, liveHeaders ? {
			status: this.#init?.status,
			statusText: this.#init?.statusText,
			headers: liveHeaders
		} : this.#init);
	}
	constructor(body, init) {
		let headers;
		this.#body = body;
		if (init instanceof GlobalResponse) {
			const cachedGlobalResponse = init[responseCache];
			if (cachedGlobalResponse) {
				this.#init = cachedGlobalResponse;
				this[getResponseCache]();
				return;
			}
			this.#init = init instanceof Response$1 ? init.#init : init;
			headers = new Headers(init.headers);
		} else this.#init = init;
		if (body == null || typeof body === "string" || typeof body?.getReader !== "undefined" || body instanceof Blob || body instanceof Uint8Array) this[cacheKey] = [
			init?.status || 200,
			body ?? null,
			headers || init?.headers
		];
	}
	get headers() {
		const cache = this[cacheKey];
		if (cache) {
			if (!(cache[2] instanceof Headers)) cache[2] = new Headers(cache[2] || (cache[1] === null ? void 0 : { "content-type": defaultContentType }));
			return cache[2];
		}
		return this[getResponseCache]().headers;
	}
	get status() {
		return this[cacheKey]?.[0] ?? this[getResponseCache]().status;
	}
	get ok() {
		const status = this.status;
		return status >= 200 && status < 300;
	}
};
[
	"body",
	"bodyUsed",
	"redirected",
	"statusText",
	"trailers",
	"type",
	"url"
].forEach((k) => {
	Object.defineProperty(Response$1.prototype, k, { get() {
		return this[getResponseCache]()[k];
	} });
});
[
	"arrayBuffer",
	"blob",
	"clone",
	"formData",
	"json",
	"text"
].forEach((k) => {
	Object.defineProperty(Response$1.prototype, k, { value: function() {
		return this[getResponseCache]()[k]();
	} });
});
Object.defineProperty(Response$1.prototype, Symbol.for("nodejs.util.inspect.custom"), { value: function(depth, options, inspectFn) {
	return `Response (lightweight) ${inspectFn({
		status: this.status,
		headers: this.headers,
		ok: this.ok,
		nativeResponse: this[responseCache]
	}, {
		...options,
		depth: depth == null ? null : depth - 1
	})}`;
} });
Object.setPrototypeOf(Response$1, GlobalResponse);
Object.setPrototypeOf(Response$1.prototype, GlobalResponse.prototype);
const validRedirectUrl = /^https?:\/\/[!#-;=?-[\]_a-z~A-Z]+$/;
const parseRedirectUrl = (url) => {
	if (url instanceof URL) return url.href;
	if (validRedirectUrl.test(url)) return url;
	return new URL(url).href;
};
const validRedirectStatuses = /* @__PURE__ */ new Set([
	301,
	302,
	303,
	307,
	308
]);
Object.defineProperty(Response$1, "redirect", {
	value: function redirect(url, status = 302) {
		if (!validRedirectStatuses.has(status)) throw new RangeError("Invalid status code");
		return new Response$1(null, {
			status,
			headers: { location: parseRedirectUrl(url) }
		});
	},
	writable: true,
	configurable: true
});
Object.defineProperty(Response$1, "json", {
	value: function json(data, init) {
		const body = JSON.stringify(data);
		if (body === void 0) throw new TypeError("The data is not JSON serializable");
		const initHeaders = init?.headers;
		let headers;
		if (initHeaders) {
			headers = new Headers(initHeaders);
			if (!headers.has("content-type")) headers.set("content-type", "application/json");
		} else headers = { "content-type": "application/json" };
		return new Response$1(body, {
			status: init?.status ?? 200,
			statusText: init?.statusText,
			headers
		});
	},
	writable: true,
	configurable: true
});
async function readWithoutBlocking(readPromise) {
	return Promise.race([readPromise, Promise.resolve().then(() => Promise.resolve(void 0))]);
}
function writeFromReadableStreamDefaultReader(reader, writable, currentReadPromise) {
	const cancel = (error) => {
		reader.cancel(error).catch(() => {});
	};
	writable.on("close", cancel);
	writable.on("error", cancel);
	(currentReadPromise ?? reader.read()).then(flow, handleStreamError);
	return reader.closed.finally(() => {
		writable.off("close", cancel);
		writable.off("error", cancel);
	});
	function handleStreamError(error) {
		if (error) writable.destroy(error);
	}
	function onDrain() {
		reader.read().then(flow, handleStreamError);
	}
	function flow({ done, value }) {
		try {
			if (done) writable.end();
			else if (!writable.write(value)) writable.once("drain", onDrain);
			else return reader.read().then(flow, handleStreamError);
		} catch (e) {
			handleStreamError(e);
		}
	}
}
function writeFromReadableStream(stream, writable) {
	if (stream.locked) throw new TypeError("ReadableStream is locked.");
	else if (writable.destroyed) return;
	return writeFromReadableStreamDefaultReader(stream.getReader(), writable);
}
const buildOutgoingHttpHeaders = (headers, defaultContentType) => {
	const res = {};
	if (!(headers instanceof Headers)) headers = new Headers(headers ?? void 0);
	if (headers.has("set-cookie")) {
		const cookies = [];
		for (const [k, v] of headers) if (k === "set-cookie") cookies.push(v);
		else res[k] = v;
		if (cookies.length > 0) res["set-cookie"] = cookies;
	} else for (const [k, v] of headers) res[k] = v;
	if (defaultContentType) res["content-type"] ??= defaultContentType;
	return res;
};
const outgoingEnded = Symbol("outgoingEnded");
const incomingDraining = Symbol("incomingDraining");
const DRAIN_TIMEOUT_MS = 500;
const MAX_DRAIN_BYTES = 64 * 1024 * 1024;
const drainIncoming = (incoming) => {
	const incomingWithDrainState = incoming;
	if (incoming.destroyed || incomingWithDrainState[incomingDraining]) return;
	incomingWithDrainState[incomingDraining] = true;
	if (incoming instanceof Http2ServerRequest) {
		try {
			incoming.stream?.close?.(constants.NGHTTP2_NO_ERROR);
		} catch {}
		return;
	}
	let bytesRead = 0;
	const cleanup = () => {
		clearTimeout(timer);
		incoming.off("data", onData);
		incoming.off("end", cleanup);
		incoming.off("error", cleanup);
	};
	const forceClose = () => {
		cleanup();
		const socket = incoming.socket;
		if (socket && !socket.destroyed) {
			if (typeof socket.destroySoon === "function") socket.destroySoon();
			else if (typeof socket.destroy === "function") socket.destroy();
		}
	};
	const timer = setTimeout(forceClose, DRAIN_TIMEOUT_MS);
	timer.unref?.();
	const onData = (chunk) => {
		bytesRead += chunk.length;
		if (bytesRead > MAX_DRAIN_BYTES) forceClose();
	};
	incoming.on("data", onData);
	incoming.on("end", cleanup);
	incoming.on("error", cleanup);
	incoming.resume();
};
const makeCloseHandler = (req, incoming, outgoing, needsBodyCleanup) => () => {
	if (incoming.errored) {
		recordBodyBufferedBeforeDisconnect(incoming);
		req[abortRequest](incoming.errored.toString());
	} else if (!outgoing.writableFinished) {
		recordBodyBufferedBeforeDisconnect(incoming);
		req[abortRequest]("Client connection prematurely closed.");
	}
	if (needsBodyCleanup && !incoming.readableEnded) setTimeout(() => {
		if (!incoming.readableEnded) setTimeout(() => {
			drainIncoming(incoming);
		});
	});
};
const isImmediateCacheableResponse = (res) => {
	if (!(cacheKey in res)) return false;
	const body = res[cacheKey][1];
	return body === null || typeof body === "string" || body instanceof Uint8Array;
};
const handleRequestError = () => new Response(null, { status: 400 });
const handleFetchError = (e) => new Response(null, { status: e instanceof Error && (e.name === "TimeoutError" || e.constructor.name === "TimeoutError") ? 504 : 500 });
const handleResponseError = (e, outgoing) => {
	const err = e instanceof Error ? e : new Error("unknown error", { cause: e });
	if (err.code === "ERR_STREAM_PREMATURE_CLOSE") console.info("The user aborted a request.");
	else {
		console.error(e);
		if (!outgoing.headersSent) {
			if (outgoing instanceof ServerResponse) outgoing._contentLength = null;
			outgoing.writeHead(500, { "Content-Type": "text/plain" });
		}
		outgoing.end(`Error: ${err.message}`);
		outgoing.destroy(err);
	}
};
const flushHeaders = (outgoing) => {
	if ("flushHeaders" in outgoing && outgoing.writable) outgoing.flushHeaders();
};
const trySetContentLength = (outgoing, status, length) => {
	const http1 = outgoing;
	if (http1._contentLength === null && http1._hasBody && http1.useChunkedEncodingByDefault && !http1._removedContLen && status >= 200 && status !== 204 && status !== 304 && !outgoing.hasHeader("content-length") && !outgoing.hasHeader("transfer-encoding") && !outgoing.hasHeader("trailer")) {
		http1._contentLength = length;
		return true;
	}
	return false;
};
const writeDefaultHeaders = (outgoing, status, length) => {
	if (trySetContentLength(outgoing, status, length)) outgoing.writeHead(status, { "Content-Type": defaultContentType });
	else outgoing.writeHead(status, {
		"Content-Type": defaultContentType,
		"Content-Length": length
	});
};
const responseViaCache = async (res, outgoing) => {
	let [status, body, header] = res[cacheKey];
	if (!header) {
		if (body === null) {
			outgoing.writeHead(status);
			outgoing.end();
		} else if (typeof body === "string") {
			writeDefaultHeaders(outgoing, status, Buffer.byteLength(body));
			outgoing.end(body);
		} else if (body instanceof Uint8Array) {
			writeDefaultHeaders(outgoing, status, body.byteLength);
			outgoing.end(body);
		} else if (body instanceof Blob) {
			writeDefaultHeaders(outgoing, status, body.size);
			outgoing.end(new Uint8Array(await body.arrayBuffer()));
		} else {
			outgoing.writeHead(status, { "Content-Type": defaultContentType });
			flushHeaders(outgoing);
			await writeFromReadableStream(body, outgoing)?.catch((e) => handleResponseError(e, outgoing));
		}
		outgoing[outgoingEnded]?.();
		return;
	}
	let hasContentLength = false;
	let plainHeaders = false;
	let canAutoLength = true;
	if (header instanceof Headers) {
		hasContentLength = header.has("content-length");
		header = buildOutgoingHttpHeaders(header, body === null ? void 0 : defaultContentType);
	} else if (Array.isArray(header)) {
		const headerObj = new Headers(header);
		hasContentLength = headerObj.has("content-length");
		header = buildOutgoingHttpHeaders(headerObj, body === null ? void 0 : defaultContentType);
	} else {
		plainHeaders = true;
		for (const key in header) {
			if (key.length === 14 && key.toLowerCase() === "content-length") {
				hasContentLength = true;
				break;
			}
			if (key.length === 17 && key.toLowerCase() === "transfer-encoding" || key.length === 7 && key.toLowerCase() === "trailer") canAutoLength = false;
		}
	}
	if (!hasContentLength) {
		let length;
		if (typeof body === "string") length = Buffer.byteLength(body);
		else if (body instanceof Uint8Array) length = body.byteLength;
		else if (body instanceof Blob) length = body.size;
		if (length !== void 0 && (!plainHeaders || !canAutoLength || !trySetContentLength(outgoing, status, length))) {
			if (plainHeaders) header = { ...header };
			header["Content-Length"] = length;
		}
	}
	outgoing.writeHead(status, header);
	if (body == null) outgoing.end();
	else if (typeof body === "string" || body instanceof Uint8Array) outgoing.end(body);
	else if (body instanceof Blob) outgoing.end(new Uint8Array(await body.arrayBuffer()));
	else {
		flushHeaders(outgoing);
		await writeFromReadableStream(body, outgoing)?.catch((e) => handleResponseError(e, outgoing));
	}
	outgoing[outgoingEnded]?.();
};
const isPromise = (res) => typeof res.then === "function";
const responseViaResponseObject = async (res, outgoing, options = {}) => {
	if (isPromise(res)) if (options.errorHandler) try {
		res = await res;
	} catch (err) {
		const errRes = await options.errorHandler(err);
		if (!errRes) return;
		res = errRes;
	}
	else res = await res.catch(handleFetchError);
	if (cacheKey in res) return responseViaCache(res, outgoing);
	const resHeaderRecord = buildOutgoingHttpHeaders(res.headers, res.body === null ? void 0 : defaultContentType);
	if (res.body) {
		const reader = res.body.getReader();
		const values = [];
		let done = false;
		let currentReadPromise = void 0;
		if (resHeaderRecord["transfer-encoding"] !== "chunked") {
			let maxReadCount = 2;
			for (let i = 0; i < maxReadCount; i++) {
				currentReadPromise ||= reader.read();
				const chunk = await readWithoutBlocking(currentReadPromise).catch((e) => {
					console.error(e);
					done = true;
				});
				if (!chunk) {
					if (i === 1) {
						await new Promise((resolve) => setTimeout(resolve));
						maxReadCount = 3;
						continue;
					}
					break;
				}
				currentReadPromise = void 0;
				if (chunk.value) values.push(chunk.value);
				if (chunk.done) {
					done = true;
					break;
				}
			}
			if (done && !("content-length" in resHeaderRecord)) resHeaderRecord["content-length"] = values.reduce((acc, value) => acc + value.length, 0);
		}
		outgoing.writeHead(res.status, resHeaderRecord);
		values.forEach((value) => {
			outgoing.write(value);
		});
		if (done) outgoing.end();
		else {
			if (values.length === 0) flushHeaders(outgoing);
			await writeFromReadableStreamDefaultReader(reader, outgoing, currentReadPromise);
		}
	} else if (resHeaderRecord["x-hono-already-sent"]) ; else {
		outgoing.writeHead(res.status, resHeaderRecord);
		outgoing.end();
	}
	outgoing[outgoingEnded]?.();
};
const getRequestListener = (fetchCallback, options = {}) => {
	const autoCleanupIncoming = options.autoCleanupIncoming ?? true;
	if (options.overrideGlobalObjects !== false && global.Request !== Request$1) {
		Object.defineProperty(global, "Request", { value: Request$1 });
		Object.defineProperty(global, "Response", { value: Response$1 });
	}
	return async (incoming, outgoing) => {
		let res, req;
		let needsBodyCleanup = false;
		let closeHandlerAttached = false;
		const ensureCloseHandler = () => {
			if (!req || closeHandlerAttached) return;
			closeHandlerAttached = true;
			outgoing.on("close", makeCloseHandler(req, incoming, outgoing, needsBodyCleanup));
		};
		try {
			req = newRequest(incoming, options.hostname);
			needsBodyCleanup = autoCleanupIncoming && !(incoming.method === "GET" || incoming.method === "HEAD");
			if (needsBodyCleanup) {
				incoming[wrapBodyStream] = true;
				if (incoming instanceof Http2ServerRequest) outgoing[outgoingEnded] = () => {
					if (!incoming.readableEnded) setTimeout(() => {
						if (!incoming.readableEnded) setTimeout(() => {
							incoming.destroy();
							outgoing.destroy();
						});
					});
				};
			}
			res = fetchCallback(req, {
				incoming,
				outgoing
			});
			if (!isPromise(res) && isImmediateCacheableResponse(res)) {
				if (needsBodyCleanup && !incoming.readableEnded) outgoing.once("finish", () => {
					if (!incoming.readableEnded) drainIncoming(incoming);
				});
				return responseViaCache(res, outgoing);
			}
			ensureCloseHandler();
		} catch (e) {
			if (!res) if (options.errorHandler) {
				ensureCloseHandler();
				res = await options.errorHandler(req ? e : toRequestError(e));
				if (!res) return;
			} else if (!req) res = handleRequestError();
			else res = handleFetchError(e);
			else return handleResponseError(e, outgoing);
		}
		try {
			return await responseViaResponseObject(res, outgoing, options);
		} catch (e) {
			return handleResponseError(e, outgoing);
		}
	};
};
const CONNECTION_SYMBOL_KEY = Symbol("CONNECTION_SYMBOL_KEY");
const WAIT_FOR_WEBSOCKET_SYMBOL = Symbol("WAIT_FOR_WEBSOCKET_SYMBOL");
const responseHeadersToSkip = /* @__PURE__ */ new Set([
	"connection",
	"content-length",
	"keep-alive",
	"proxy-authenticate",
	"proxy-authorization",
	"te",
	"trailer",
	"transfer-encoding",
	"upgrade",
	"sec-websocket-accept",
	"sec-websocket-extensions",
	"sec-websocket-protocol"
]);
const appendResponseHeaders = (headers, responseHeaders) => {
	if (!responseHeaders) return;
	responseHeaders.forEach((value, key) => {
		if (responseHeadersToSkip.has(key.toLowerCase())) return;
		headers.push(`${key}: ${value}`);
	});
};
const rejectUpgradeRequest = (socket, status, responseHeaders) => {
	const responseLines = ["Connection: close", "Content-Length: 0"];
	appendResponseHeaders(responseLines, responseHeaders);
	socket.end(`HTTP/1.1 ${status.toString()} ${STATUS_CODES[status] ?? ""}\r\n${responseLines.join("\r\n")}\r\n\r
`);
};
const createUpgradeRequest = (request) => {
	const protocol = request.socket.encrypted ? "https" : "http";
	const url = new URL(request.url ?? "/", `${protocol}://${request.headers.host ?? "localhost"}`);
	const headers = new Headers();
	for (const key in request.headers) {
		const value = request.headers[key];
		if (!value) continue;
		headers.append(key, Array.isArray(value) ? value[0] : value);
	}
	return new Request(url, { headers });
};
const setupWebSocket = (options) => {
	const { server, fetchCallback, wss } = options;
	const waiterMap = /* @__PURE__ */ new Map();
	wss.on("connection", (ws, request) => {
		const waiter = waiterMap.get(request);
		if (waiter) {
			waiter.resolve(ws);
			waiterMap.delete(request);
		}
	});
	const rejectWaiter = (request) => {
		const waiter = waiterMap.get(request);
		if (waiter) {
			waiterMap.delete(request);
			waiter.reject(/* @__PURE__ */ new Error("WebSocket handshake aborted"));
		}
	};
	const waitForWebSocket = (request, connectionSymbol) => {
		return new Promise((resolve, reject) => {
			waiterMap.set(request, {
				resolve,
				reject,
				connectionSymbol
			});
		});
	};
	server.on("upgrade", async (request, socket, head) => {
		if (request.headers.upgrade?.toLowerCase() !== "websocket") return;
		const env = {
			incoming: request,
			outgoing: void 0,
			wss,
			[WAIT_FOR_WEBSOCKET_SYMBOL]: waitForWebSocket
		};
		let status = 400;
		let responseHeaders;
		try {
			const response = await fetchCallback(createUpgradeRequest(request), env);
			if (response instanceof Response) {
				status = response.status;
				responseHeaders = response.headers;
			}
		} catch {
			if (server.listenerCount("upgrade") === 1) rejectUpgradeRequest(socket, 500);
			return;
		}
		const waiter = waiterMap.get(request);
		if (!waiter || waiter.connectionSymbol !== env[CONNECTION_SYMBOL_KEY]) {
			rejectWaiter(request);
			if (server.listenerCount("upgrade") === 1) rejectUpgradeRequest(socket, status, responseHeaders);
			return;
		}
		const addResponseHeaders = (headers) => {
			appendResponseHeaders(headers, responseHeaders);
		};
		const reclaimWaiterOnClose = () => rejectWaiter(request);
		socket.once("close", reclaimWaiterOnClose);
		wss.on("headers", addResponseHeaders);
		try {
			wss.handleUpgrade(request, socket, head, (ws) => {
				socket.off("close", reclaimWaiterOnClose);
				wss.emit("connection", ws, request);
			});
		} finally {
			wss.off("headers", addResponseHeaders);
		}
	});
	server.on("close", () => {
		wss.close();
	});
};
const createAdaptorServer = (options) => {
	const fetchCallback = options.fetch;
	const requestListener = getRequestListener(fetchCallback, {
		hostname: options.hostname,
		overrideGlobalObjects: options.overrideGlobalObjects,
		autoCleanupIncoming: options.autoCleanupIncoming
	});
	const server = (options.createServer || createServer)(options.serverOptions || {}, requestListener);
	if (options.websocket && options.websocket.server) {
		if (options.websocket.server.options.noServer !== true) throw new Error("WebSocket server must be created with { noServer: true } option");
		setupWebSocket({
			server,
			fetchCallback,
			wss: options.websocket.server
		});
	}
	return server;
};
const serve = (options, listeningListener) => {
	const server = createAdaptorServer(options);
	server.listen(options?.port ?? 3e3, options.hostname, () => {
		const serverInfo = server.address();
		listeningListener && listeningListener(serverInfo);
	});
	return server;
};
//#endregion
//#region ../../../../../setup-pnpm/node_modules/.bin/store/v11/links/@hono/node-server/2.1.3/77fb7b7d8de4d768581a422b8f5a10243114c5fe830d14a0f6e462670caab44a/node_modules/@hono/node-server/dist/utils/stream.mjs
const pr54206Applied = () => {
	const [major, minor] = versions.node.split(".").map((component) => parseInt(component));
	return major >= 23 || major === 22 && minor >= 7 || major === 20 && minor >= 18;
};
const useReadableToWeb = pr54206Applied();
const createStreamBody = (stream, useNativeReadableToWeb = useReadableToWeb) => {
	if (useNativeReadableToWeb) return Readable.toWeb(stream);
	let controller;
	let settled = false;
	const cleanup = () => {
		stream.off("data", onData);
		stream.off("error", onError);
		stream.off("end", onTerminate);
		stream.off("close", onTerminate);
	};
	const settle = (callback) => {
		if (settled) return;
		settled = true;
		cleanup();
		callback?.();
	};
	const onData = (chunk) => {
		if (settled || !controller) return;
		controller.enqueue(chunk);
		if ((controller.desiredSize ?? 0) <= 0) stream.pause();
	};
	const onError = (error) => {
		settle(() => {
			controller?.error(error);
		});
	};
	const onTerminate = () => {
		settle(() => {
			controller?.close();
		});
	};
	return new ReadableStream({
		start(streamController) {
			controller = streamController;
			stream.on("data", onData);
			stream.on("error", onError);
			stream.on("end", onTerminate);
			stream.on("close", onTerminate);
			stream.pause();
		},
		pull() {
			if (!settled) stream.resume();
		},
		cancel() {
			settle();
			const ignoreError = () => {};
			stream.on("error", ignoreError);
			stream.once("close", () => stream.off("error", ignoreError));
			stream.destroy();
		}
	});
};
//#endregion
//#region ../../../../../setup-pnpm/node_modules/.bin/store/v11/links/@hono/node-server/2.1.3/77fb7b7d8de4d768581a422b8f5a10243114c5fe830d14a0f6e462670caab44a/node_modules/@hono/node-server/dist/serve-static.mjs
const COMPRESSIBLE_CONTENT_TYPE_REGEX = /^\s*(?:text\/[^;\s]+|application\/(?:javascript|json|xml|xml-dtd|ecmascript|dart|postscript|rtf|tar|toml|vnd\.dart|vnd\.ms-fontobject|vnd\.ms-opentype|wasm|x-httpd-php|x-javascript|x-ns-proxy-autoconfig|x-sh|x-tar|x-virtualbox-hdd|x-virtualbox-ova|x-virtualbox-ovf|x-virtualbox-vbox|x-virtualbox-vdi|x-virtualbox-vhd|x-virtualbox-vmdk|x-www-form-urlencoded)|font\/(?:otf|ttf)|image\/(?:bmp|vnd\.adobe\.photoshop|vnd\.microsoft\.icon|vnd\.ms-dds|x-icon|x-ms-bmp)|message\/rfc822|model\/gltf-binary|x-shader\/x-fragment|x-shader\/x-vertex|[^;\s]+?\+(?:json|text|xml|yaml))(?:[;\s]|$)/i;
const ENCODINGS = {
	br: ".br",
	zstd: ".zst",
	gzip: ".gz"
};
const ENCODINGS_ORDERED_KEYS = Object.keys(ENCODINGS);
const getStats = (path) => {
	let stats;
	try {
		stats = statSync(path);
	} catch {}
	return stats;
};
const BYTE_RANGE_PATTERN = /^(?:bytes=)?(?!-$)(\d*)-(\d*)$/;
const parseByteRange = (range) => {
	const match = range.match(BYTE_RANGE_PATTERN);
	if (!match) return;
	const [, start, end] = match;
	if (start === "") return {
		type: "suffix",
		length: Number(end)
	};
	if (end === "") return {
		type: "open-ended",
		start: Number(start)
	};
	return {
		type: "bounded",
		start: Number(start),
		end: Number(end)
	};
};
const resolveByteRange = (spec, size) => {
	if (size === 0) return;
	if (spec.type === "suffix") {
		if (spec.length === 0) return;
		return {
			start: Math.max(size - spec.length, 0),
			end: size - 1
		};
	}
	const end = spec.type === "bounded" ? Math.min(spec.end, size - 1) : size - 1;
	if (spec.start >= size || spec.start > end) return;
	return {
		start: spec.start,
		end
	};
};
const tryDecode = (str, decoder) => {
	try {
		return decoder(str);
	} catch {
		return str.replace(/(?:%[0-9A-Fa-f]{2})+/g, (match) => {
			try {
				return decoder(match);
			} catch {
				return match;
			}
		});
	}
};
const tryDecodeURI = (str) => tryDecode(str, decodeURI);
const serveStatic = (options = { root: "" }) => {
	const root = options.root || "";
	const optionPath = options.path;
	if (root !== "" && !existsSync(root)) console.error(`serveStatic: root path '${root}' is not found, are you sure it's correct?`);
	return async (c, next) => {
		if (c.finalized) return next();
		let filename;
		if (optionPath) filename = optionPath;
		else try {
			if (!options.allowPercentInPath && c.req.path.includes("%")) throw new Error();
			filename = tryDecodeURI(c.req.path);
			if (/(?:^|[\/\\])\.{1,2}(?:$|[\/\\])|[\/\\]{2,}|\\/.test(filename)) throw new Error();
		} catch {
			await options.onNotFound?.(c.req.path, c);
			return next();
		}
		let path = join(root, !optionPath && options.rewriteRequestPath ? options.rewriteRequestPath(filename, c) : filename);
		let stats = getStats(path);
		if (stats && stats.isDirectory()) {
			const indexFile = options.index ?? "index.html";
			path = join(path, indexFile);
			stats = getStats(path);
		}
		if (!stats) {
			await options.onNotFound?.(path, c);
			return next();
		}
		const mimeType = getMimeType(path);
		c.header("Content-Type", mimeType || "application/octet-stream");
		if (options.precompressed && (!mimeType || mimeType === "application/octet-stream" || COMPRESSIBLE_CONTENT_TYPE_REGEX.test(mimeType))) {
			const acceptEncodingSet = new Set(c.req.header("Accept-Encoding")?.split(",").map((encoding) => encoding.trim()));
			for (const encoding of ENCODINGS_ORDERED_KEYS) {
				if (!acceptEncodingSet.has(encoding)) continue;
				const precompressedStats = getStats(path + ENCODINGS[encoding]);
				if (precompressedStats) {
					c.header("Content-Encoding", encoding);
					c.header("Vary", "Accept-Encoding", { append: true });
					stats = precompressedStats;
					path = path + ENCODINGS[encoding];
					break;
				}
			}
		}
		let result;
		const size = stats.size;
		const range = c.req.header("range") || "";
		c.header("Last-Modified", stats.mtime.toUTCString());
		if (c.req.method == "HEAD" || c.req.method == "OPTIONS") {
			c.header("Content-Length", size.toString());
			c.status(200);
			result = c.body(null);
		} else if (!range) {
			c.header("Content-Length", size.toString());
			result = c.body(createStreamBody(createReadStream(path)), 200);
		} else {
			c.header("Accept-Ranges", "bytes");
			const resolvedRange = resolveByteRange(parseByteRange(range) ?? {
				type: "open-ended",
				start: 0
			}, size);
			if (!resolvedRange) {
				c.header("Content-Range", `bytes */${size}`);
				result = c.body(null, 416);
			} else {
				const { start, end } = resolvedRange;
				const chunkSize = end - start + 1;
				const stream = createReadStream(path, {
					start,
					end
				});
				c.header("Content-Length", chunkSize.toString());
				c.header("Content-Range", `bytes ${start}-${end}/${size}`);
				result = c.body(createStreamBody(stream), 206);
			}
		}
		await options.onFound?.(path, c);
		return result;
	};
};
//#endregion
//#region ../../../../../setup-pnpm/node_modules/.bin/store/v11/links/@hono/swagger-ui/0.5.3/7b19cbfa9f0684e68bc398d56564591a4cdbcb67a12950a659184a25749b2d30/node_modules/@hono/swagger-ui/dist/index.js
const RENDER_TYPE = {
	STRING_ARRAY: "string_array",
	STRING: "string",
	JSON_STRING: "json_string",
	RAW: "raw"
};
const RENDER_TYPE_MAP = {
	configUrl: RENDER_TYPE.STRING,
	deepLinking: RENDER_TYPE.RAW,
	presets: RENDER_TYPE.STRING_ARRAY,
	plugins: RENDER_TYPE.STRING_ARRAY,
	spec: RENDER_TYPE.JSON_STRING,
	url: RENDER_TYPE.STRING,
	urls: RENDER_TYPE.JSON_STRING,
	layout: RENDER_TYPE.STRING,
	docExpansion: RENDER_TYPE.STRING,
	maxDisplayedTags: RENDER_TYPE.RAW,
	operationsSorter: RENDER_TYPE.RAW,
	requestInterceptor: RENDER_TYPE.RAW,
	responseInterceptor: RENDER_TYPE.RAW,
	persistAuthorization: RENDER_TYPE.RAW,
	defaultModelsExpandDepth: RENDER_TYPE.RAW,
	defaultModelExpandDepth: RENDER_TYPE.RAW,
	defaultModelRendering: RENDER_TYPE.STRING,
	displayRequestDuration: RENDER_TYPE.RAW,
	filter: RENDER_TYPE.RAW,
	showExtensions: RENDER_TYPE.RAW,
	showCommonExtensions: RENDER_TYPE.RAW,
	queryConfigEnabled: RENDER_TYPE.RAW,
	displayOperationId: RENDER_TYPE.RAW,
	tagsSorter: RENDER_TYPE.RAW,
	onComplete: RENDER_TYPE.RAW,
	syntaxHighlight: RENDER_TYPE.JSON_STRING,
	tryItOutEnabled: RENDER_TYPE.RAW,
	requestSnippetsEnabled: RENDER_TYPE.RAW,
	requestSnippets: RENDER_TYPE.JSON_STRING,
	oauth2RedirectUrl: RENDER_TYPE.STRING,
	showMutabledRequest: RENDER_TYPE.RAW,
	request: RENDER_TYPE.JSON_STRING,
	supportedSubmitMethods: RENDER_TYPE.JSON_STRING,
	validatorUrl: RENDER_TYPE.STRING,
	withCredentials: RENDER_TYPE.RAW,
	modelPropertyMacro: RENDER_TYPE.RAW,
	parameterMacro: RENDER_TYPE.RAW
};
const renderSwaggerUIOptions = (options) => {
	return Object.entries(options).map(([k, v]) => {
		const key = k;
		if (!RENDER_TYPE_MAP[key] || v === void 0) return "";
		switch (RENDER_TYPE_MAP[key]) {
			case RENDER_TYPE.STRING: return `${key}: '${v}'`;
			case RENDER_TYPE.STRING_ARRAY:
				if (!Array.isArray(v)) return "";
				return `${key}: [${v.map((ve) => `${ve}`).join(",")}]`;
			case RENDER_TYPE.JSON_STRING: return `${key}: ${JSON.stringify(v)}`;
			case RENDER_TYPE.RAW: return `${key}: ${v}`;
			default: return "";
		}
	}).filter((item) => item !== "").join(",");
};
const remoteAssets = ({ version }) => {
	const url = `https://cdn.jsdelivr.net/npm/swagger-ui-dist${version !== void 0 ? `@${version}` : ""}`;
	return {
		css: [`${url}/swagger-ui.css`],
		js: [`${url}/swagger-ui-bundle.js`]
	};
};
const SwaggerUI = (options) => {
	const asset = remoteAssets({ version: options?.version });
	delete options.version;
	if (options.manuallySwaggerUIHtml) return options.manuallySwaggerUIHtml(asset);
	const optionsStrings = renderSwaggerUIOptions(options);
	return `
    <div>
      <div id="swagger-ui"></div>
      ${asset.css.map((url) => html`<link rel="stylesheet" href="${url}" />`)}
      ${asset.js.map((url) => html`<script src="${url}" crossorigin="anonymous"><\/script>`)}
      <script>
        window.onload = () => {
          window.ui = SwaggerUIBundle({
            dom_id: '#swagger-ui',${optionsStrings},
          })
        }
      <\/script>
    </div>
  `;
};
const middleware = (options) => async (c) => {
	const title = options?.title ?? "SwaggerUI";
	return c.html(`
      <!doctype html>
      <html lang="en">
        <head>
          <meta charset="utf-8" />
          <meta name="viewport" content="width=device-width, initial-scale=1" />
          <meta name="description" content="SwaggerUI" />
          <title>${title}</title>
        </head>
        <body>
          ${SwaggerUI(options)}
        </body>
      </html>
    `);
};
//#endregion
//#region ../../../../../setup-pnpm/node_modules/.bin/store/v11/links/@/fetch-to-node/2.1.0/75a085586b95f3a18a440f8655a7cb5c851846ab2c5e6ad2c4ae036e3bbaa05c/node_modules/fetch-to-node/dist/utils/errors.js
const classRegExp = /^([A-Z][a-z0-9]*)+$/;
const kTypes = [
	"string",
	"function",
	"number",
	"object",
	"Function",
	"Object",
	"boolean",
	"bigint",
	"symbol"
];
/**
* Determine the specific type of a value for type-mismatch errors.
* @param {*} value
* @returns {string}
*/
function determineSpecificType(value) {
	if (value == null) return "" + value;
	if (typeof value === "function" && value.name) return `function ${value.name}`;
	if (typeof value === "object") {
		if (value.constructor?.name) return `an instance of ${value.constructor.name}`;
		return `${util.inspect(value, { depth: -1 })}`;
	}
	let inspected = util.inspect(value, { colors: false });
	if (inspected.length > 28) inspected = `${inspected.slice(0, 25)}...`;
	return `type ${typeof value} (${inspected})`;
}
var ERR_HTTP_BODY_NOT_ALLOWED = class extends Error {
	constructor() {
		super("Adding content for this request method or response status is not allowed.");
	}
};
var ERR_HTTP_CONTENT_LENGTH_MISMATCH = class extends Error {
	constructor(actual, expected) {
		super(`Response body's content-length of ${actual} byte(s) does not match the content-length of ${expected} byte(s) set in header`);
	}
};
var ERR_HTTP_HEADERS_SENT = class extends Error {
	constructor(arg) {
		super(`Cannot ${arg} headers after they are sent to the client`);
	}
};
var ERR_INVALID_ARG_VALUE = class extends TypeError {
	constructor(name, value, reason = "is invalid") {
		let inspected = util.inspect(value);
		if (inspected.length > 128) inspected = `${inspected.slice(0, 128)}...`;
		const type = name.includes(".") ? "property" : "argument";
		super(`The ${type} '${name}' ${reason}. Received ${inspected}`);
	}
};
var ERR_INVALID_CHAR = class extends TypeError {
	constructor(name, field) {
		let msg = `Invalid character in ${name}`;
		if (field !== void 0) msg += ` ["${field}"]`;
		super(msg);
	}
};
var ERR_HTTP_INVALID_HEADER_VALUE = class extends TypeError {
	constructor(value, name) {
		super(`Invalid value "${value}" for header "${name}"`);
	}
};
var ERR_HTTP_INVALID_STATUS_CODE = class extends RangeError {
	originalStatusCode;
	constructor(originalStatusCode) {
		super(`Invalid status code: ${originalStatusCode}`);
		this.originalStatusCode = originalStatusCode;
	}
};
var ERR_HTTP_TRAILER_INVALID = class extends Error {
	constructor() {
		super(`Trailers are invalid with this transfer encoding`);
	}
};
var ERR_INVALID_ARG_TYPE = class extends TypeError {
	constructor(name, expected, actual) {
		if (!Array.isArray(expected)) expected = [expected];
		let msg = "The ";
		if (name.endsWith(" argument")) msg += `${name} `;
		else {
			const type = name.includes(".") ? "property" : "argument";
			msg += `"${name}" ${type} `;
		}
		msg += "must be ";
		const types = [];
		const instances = [];
		const other = [];
		for (const value of expected) if (kTypes.includes(value)) types.push(value.toLowerCase());
		else if (classRegExp.exec(value) !== null) instances.push(value);
		else other.push(value);
		if (instances.length > 0) {
			const pos = types.indexOf("object");
			if (pos !== -1) {
				types.splice(pos, 1);
				instances.push("Object");
			}
		}
		if (types.length > 0) {
			if (types.length > 2) {
				const last = types.pop();
				msg += `one of type ${types.join(", ")}, or ${last}`;
			} else if (types.length === 2) msg += `one of type ${types[0]} or ${types[1]}`;
			else msg += `of type ${types[0]}`;
			if (instances.length > 0 || other.length > 0) msg += " or ";
		}
		if (instances.length > 0) {
			if (instances.length > 2) {
				const last = instances.pop();
				msg += `an instance of ${instances.join(", ")}, or ${last}`;
			} else {
				msg += `an instance of ${instances[0]}`;
				if (instances.length === 2) msg += ` or ${instances[1]}`;
			}
			if (other.length > 0) msg += " or ";
		}
		if (other.length > 0) if (other.length > 2) {
			const last = other.pop();
			msg += `one of ${other.join(", ")}, or ${last}`;
		} else if (other.length === 2) msg += `one of ${other[0]} or ${other[1]}`;
		else {
			if (other[0].toLowerCase() !== other[0]) msg += "an ";
			msg += `${other[0]}`;
		}
		msg += `. Received ${determineSpecificType(actual)}`;
		super(msg);
	}
};
var ERR_INVALID_HTTP_TOKEN = class extends TypeError {
	constructor(name, field) {
		super(`${name} must be a valid HTTP token ["${field}"]`);
	}
};
var ERR_METHOD_NOT_IMPLEMENTED = class extends Error {
	constructor(methodName) {
		super(`The ${methodName} method is not implemented`);
	}
};
var ERR_STREAM_ALREADY_FINISHED = class extends Error {
	constructor(methodName) {
		super(`Cannot call ${methodName} after a stream was finished`);
	}
};
var ERR_STREAM_CANNOT_PIPE = class extends Error {
	constructor() {
		super(`Cannot pipe, not readable`);
	}
};
var ERR_STREAM_DESTROYED = class extends Error {
	constructor(methodName) {
		super(`Cannot call ${methodName} after a stream was destroyed`);
	}
};
var ERR_STREAM_NULL_VALUES = class extends TypeError {
	constructor() {
		super(`May not write null values to stream`);
	}
};
var ERR_STREAM_WRITE_AFTER_END = class extends Error {
	constructor() {
		super(`write after end`);
	}
};
//#endregion
//#region ../../../../../setup-pnpm/node_modules/.bin/store/v11/links/@/fetch-to-node/2.1.0/75a085586b95f3a18a440f8655a7cb5c851846ab2c5e6ad2c4ae036e3bbaa05c/node_modules/fetch-to-node/dist/fetch-to-node/http-incoming.js
const kHeaders = Symbol("kHeaders");
const kHeadersDistinct = Symbol("kHeadersDistinct");
const kHeadersCount = Symbol("kHeadersCount");
const kTrailers = Symbol("kTrailers");
const kTrailersDistinct = Symbol("kTrailersDistinct");
const kTrailersCount = Symbol("kTrailersCount");
/**
* This is an implementation of IncomingMessage from Node.js intended to run in
* a WinterTC runtime. The 'Readable' interface of this class is wired to a 'Request'
* object's 'body'.
*
* This instance can be used in normal ways, but it does not give access to the
* underlying socket (because there isn't one. req.socket will always return null).
*
* Some code in this class is transplanted/adapted from node/lib/_http_incoming.js
*/
var FetchIncomingMessage = class extends Readable {
	get socket() {
		return null;
	}
	set socket(_val) {
		throw new ERR_METHOD_NOT_IMPLEMENTED("socket");
	}
	httpVersionMajor;
	httpVersionMinor;
	httpVersion;
	complete = false;
	[kHeaders] = null;
	[kHeadersDistinct] = null;
	[kHeadersCount] = 0;
	rawHeaders = [];
	[kTrailers] = null;
	[kTrailersDistinct] = null;
	[kTrailersCount] = 0;
	rawTrailers = [];
	joinDuplicateHeaders = false;
	aborted = false;
	upgrade = false;
	url = "";
	method;
	_consuming;
	_dumped;
	_stream = null;
	constructor() {
		super({});
		this._readableState.readingMore = true;
		this._consuming = false;
		this._dumped = false;
	}
	get connection() {
		return null;
	}
	set connection(_socket) {
		console.error("No support for IncomingMessage.connection");
	}
	get headers() {
		if (!this[kHeaders]) {
			this[kHeaders] = {};
			const src = this.rawHeaders;
			const dst = this[kHeaders];
			for (let n = 0; n < this[kHeadersCount]; n += 2) this._addHeaderLine(src[n], src[n + 1], dst);
		}
		return this[kHeaders];
	}
	set headers(val) {
		this[kHeaders] = val;
	}
	get headersDistinct() {
		if (!this[kHeadersDistinct]) {
			this[kHeadersDistinct] = {};
			const src = this.rawHeaders;
			const dst = this[kHeadersDistinct];
			for (let n = 0; n < this[kHeadersCount]; n += 2) this._addHeaderLineDistinct(src[n], src[n + 1], dst);
		}
		return this[kHeadersDistinct];
	}
	set headersDistinct(val) {
		this[kHeadersDistinct] = val;
	}
	get trailers() {
		if (!this[kTrailers]) {
			this[kTrailers] = {};
			const src = this.rawTrailers;
			const dst = this[kTrailers];
			for (let n = 0; n < this[kTrailersCount]; n += 2) this._addHeaderLine(src[n], src[n + 1], dst);
		}
		return this[kTrailers];
	}
	set trailers(val) {
		this[kTrailers] = val;
	}
	get trailersDistinct() {
		if (!this[kTrailersDistinct]) {
			this[kTrailersDistinct] = {};
			const src = this.rawTrailers;
			const dst = this[kTrailersDistinct];
			for (let n = 0; n < this[kTrailersCount]; n += 2) this._addHeaderLineDistinct(src[n], src[n + 1], dst);
		}
		return this[kTrailersDistinct];
	}
	set trailersDistinct(val) {
		this[kTrailersDistinct] = val;
	}
	setTimeout(msecs, callback) {
		return this;
	}
	async _read(n) {
		if (!this._consuming) {
			this._readableState.readingMore = false;
			this._consuming = true;
		}
		if (this._stream == null) {
			this.complete = true;
			this.push(null);
			return;
		}
		const reader = this._stream.getReader();
		try {
			const data = await reader.read();
			if (data.done) {
				this.complete = true;
				this.push(null);
			} else this.push(data.value);
		} catch (e) {
			this.destroy(e);
		} finally {
			reader.releaseLock();
		}
	}
	_destroy(err, cb) {
		if (!this.readableEnded || !this.complete) {
			this.aborted = true;
			this.emit("aborted");
		}
		setTimeout(onError$1, 0, this, err, cb);
	}
	_addHeaderLines(headers, n) {
		if (headers?.length) {
			let dest;
			if (this.complete) {
				this.rawTrailers = headers;
				this[kTrailersCount] = n;
				dest = this[kTrailers];
			} else {
				this.rawHeaders = headers;
				this[kHeadersCount] = n;
				dest = this[kHeaders];
			}
			if (dest) for (let i = 0; i < n; i += 2) this._addHeaderLine(headers[i], headers[i + 1], dest);
		}
	}
	_addHeaderLine(field, value, dest) {
		field = matchKnownFields(field);
		const flag = field.charCodeAt(0);
		if (flag === 0 || flag === 2) {
			field = field.slice(1);
			if (typeof dest[field] === "string") dest[field] += (flag === 0 ? ", " : "; ") + value;
			else dest[field] = value;
		} else if (flag === 1) if (dest["set-cookie"] !== void 0) dest["set-cookie"].push(value);
		else dest["set-cookie"] = [value];
		else if (this.joinDuplicateHeaders) if (dest[field] === void 0) dest[field] = value;
		else dest[field] += ", " + value;
		else if (dest[field] === void 0) dest[field] = value;
	}
	_addHeaderLineDistinct(field, value, dest) {
		field = field.toLowerCase();
		if (!dest[field]) dest[field] = [value];
		else dest[field].push(value);
	}
	_dump() {
		if (!this._dumped) {
			this._dumped = true;
			this.removeAllListeners("data");
			this.resume();
		}
	}
};
function matchKnownFields(field, lowercased = false) {
	switch (field.length) {
		case 3:
			if (field === "Age" || field === "age") return "age";
			break;
		case 4:
			if (field === "Host" || field === "host") return "host";
			if (field === "From" || field === "from") return "from";
			if (field === "ETag" || field === "etag") return "etag";
			if (field === "Date" || field === "date") return "\0date";
			if (field === "Vary" || field === "vary") return "\0vary";
			break;
		case 6:
			if (field === "Server" || field === "server") return "server";
			if (field === "Cookie" || field === "cookie") return "cookie";
			if (field === "Origin" || field === "origin") return "\0origin";
			if (field === "Expect" || field === "expect") return "\0expect";
			if (field === "Accept" || field === "accept") return "\0accept";
			break;
		case 7:
			if (field === "Referer" || field === "referer") return "referer";
			if (field === "Expires" || field === "expires") return "expires";
			if (field === "Upgrade" || field === "upgrade") return "\0upgrade";
			break;
		case 8:
			if (field === "Location" || field === "location") return "location";
			if (field === "If-Match" || field === "if-match") return "\0if-match";
			break;
		case 10:
			if (field === "User-Agent" || field === "user-agent") return "user-agent";
			if (field === "Set-Cookie" || field === "set-cookie") return "";
			if (field === "Connection" || field === "connection") return "\0connection";
			break;
		case 11:
			if (field === "Retry-After" || field === "retry-after") return "retry-after";
			break;
		case 12:
			if (field === "Content-Type" || field === "content-type") return "content-type";
			if (field === "Max-Forwards" || field === "max-forwards") return "max-forwards";
			break;
		case 13:
			if (field === "Authorization" || field === "authorization") return "authorization";
			if (field === "Last-Modified" || field === "last-modified") return "last-modified";
			if (field === "Cache-Control" || field === "cache-control") return "\0cache-control";
			if (field === "If-None-Match" || field === "if-none-match") return "\0if-none-match";
			break;
		case 14:
			if (field === "Content-Length" || field === "content-length") return "content-length";
			break;
		case 15:
			if (field === "Accept-Encoding" || field === "accept-encoding") return "\0accept-encoding";
			if (field === "Accept-Language" || field === "accept-language") return "\0accept-language";
			if (field === "X-Forwarded-For" || field === "x-forwarded-for") return "\0x-forwarded-for";
			break;
		case 16:
			if (field === "Content-Encoding" || field === "content-encoding") return "\0content-encoding";
			if (field === "X-Forwarded-Host" || field === "x-forwarded-host") return "\0x-forwarded-host";
			break;
		case 17:
			if (field === "If-Modified-Since" || field === "if-modified-since") return "if-modified-since";
			if (field === "Transfer-Encoding" || field === "transfer-encoding") return "\0transfer-encoding";
			if (field === "X-Forwarded-Proto" || field === "x-forwarded-proto") return "\0x-forwarded-proto";
			break;
		case 19:
			if (field === "Proxy-Authorization" || field === "proxy-authorization") return "proxy-authorization";
			if (field === "If-Unmodified-Since" || field === "if-unmodified-since") return "if-unmodified-since";
			break;
	}
	if (lowercased) return "\0" + field;
	return matchKnownFields(field.toLowerCase(), true);
}
function onError$1(self, error, cb) {
	if (self.listenerCount("error") === 0) cb();
	else cb(error);
}
//#endregion
//#region ../../../../../setup-pnpm/node_modules/.bin/store/v11/links/@/fetch-to-node/2.1.0/75a085586b95f3a18a440f8655a7cb5c851846ab2c5e6ad2c4ae036e3bbaa05c/node_modules/fetch-to-node/dist/utils/types.js
function validateString(value, name) {
	if (typeof value !== "string") throw new ERR_INVALID_ARG_TYPE(name, "string", value);
}
const linkValueRegExp = /^(?:<[^>]*>)(?:\s*;\s*[^;"\s]+(?:=(")?[^;"\s]*\1)?)*$/;
function validateLinkHeaderFormat(value, name) {
	if (typeof value === "undefined" || !linkValueRegExp.exec(value)) throw new ERR_INVALID_ARG_VALUE(name, value, "must be an array or string of format \"</styles.css>; rel=preload; as=style\"");
}
function validateLinkHeaderValue(hints) {
	if (typeof hints === "string") {
		validateLinkHeaderFormat(hints, "hints");
		return hints;
	} else if (Array.isArray(hints)) {
		const hintsLength = hints.length;
		let result = "";
		if (hintsLength === 0) return result;
		for (let i = 0; i < hintsLength; i++) {
			const link = hints[i];
			validateLinkHeaderFormat(link, "hints");
			result += link;
			if (i !== hintsLength - 1) result += ", ";
		}
		return result;
	}
	throw new ERR_INVALID_ARG_VALUE("hints", hints, "must be an array or string of format \"</styles.css>; rel=preload; as=style\"");
}
function isUint8Array(value) {
	return value != null && value[Symbol.toStringTag] === "Uint8Array";
}
//#endregion
//#region ../../../../../setup-pnpm/node_modules/.bin/store/v11/links/@/fetch-to-node/2.1.0/75a085586b95f3a18a440f8655a7cb5c851846ab2c5e6ad2c4ae036e3bbaa05c/node_modules/fetch-to-node/dist/fetch-to-node/internal-http.js
const kNeedDrain = Symbol("kNeedDrain");
const kOutHeaders = Symbol("kOutHeaders");
function utcDate() {
	return (/* @__PURE__ */ new Date()).toUTCString();
}
//#endregion
//#region ../../../../../setup-pnpm/node_modules/.bin/store/v11/links/@/fetch-to-node/2.1.0/75a085586b95f3a18a440f8655a7cb5c851846ab2c5e6ad2c4ae036e3bbaa05c/node_modules/fetch-to-node/dist/fetch-to-node/internal-streams-state.js
function getDefaultHighWaterMark(objectMode) {
	return objectMode ? 16 : 64 * 1024;
}
//#endregion
//#region ../../../../../setup-pnpm/node_modules/.bin/store/v11/links/@/fetch-to-node/2.1.0/75a085586b95f3a18a440f8655a7cb5c851846ab2c5e6ad2c4ae036e3bbaa05c/node_modules/fetch-to-node/dist/fetch-to-node/http-common.js
const tokenRegExp = /^[\^_`a-zA-Z\-0-9!#$%&'*+.|~]+$/;
/**
* Verifies that the given val is a valid HTTP token
* per the rules defined in RFC 7230
* See https://tools.ietf.org/html/rfc7230#section-3.2.6
*/
function checkIsHttpToken(val) {
	return tokenRegExp.test(val);
}
const headerCharRegex$1 = /[^\t\x20-\x7e\x80-\xff]/;
/**
* True if val contains an invalid field-vchar
*  field-value    = *( field-content / obs-fold )
*  field-content  = field-vchar [ 1*( SP / HTAB ) field-vchar ]
*  field-vchar    = VCHAR / obs-text
*/
function checkInvalidHeaderChar$1(val) {
	return headerCharRegex$1.test(val);
}
const chunkExpression = /(?:^|\W)chunked(?:$|\W)/i;
//#endregion
//#region ../../../../../setup-pnpm/node_modules/.bin/store/v11/links/@/fetch-to-node/2.1.0/75a085586b95f3a18a440f8655a7cb5c851846ab2c5e6ad2c4ae036e3bbaa05c/node_modules/fetch-to-node/dist/fetch-to-node/http-outgoing.js
const kCorked = Symbol("corked");
const kChunkedBuffer = Symbol("kChunkedBuffer");
const kChunkedLength = Symbol("kChunkedLength");
const kUniqueHeaders = Symbol("kUniqueHeaders");
const kBytesWritten = Symbol("kBytesWritten");
const kErrored = Symbol("errored");
const kHighWaterMark = Symbol("kHighWaterMark");
const kRejectNonStandardBodyWrites = Symbol("kRejectNonStandardBodyWrites");
const nop = () => {};
const RE_CONN_CLOSE = /(?:^|\W)close(?:$|\W)/i;
function isCookieField(s) {
	return s.length === 6 && s.toLowerCase() === "cookie";
}
function isContentDispositionField(s) {
	return s.length === 19 && s.toLowerCase() === "content-disposition";
}
/**
* An in-memory buffer that stores the chunks that have been streamed to an
* OutgoingMessage instance.
*/
var WrittenDataBuffer = class {
	[kCorked] = 0;
	[kHighWaterMark] = getDefaultHighWaterMark();
	entries = [];
	onWrite;
	constructor(params = {}) {
		this.onWrite = params.onWrite;
	}
	write(data, encoding, callback) {
		this.entries.push({
			data,
			length: data.length,
			encoding,
			callback,
			written: false
		});
		this._flush();
		return true;
	}
	cork() {
		this[kCorked]++;
	}
	uncork() {
		this[kCorked]--;
		this._flush();
	}
	_flush() {
		if (this[kCorked] <= 0) {
			for (const [index, entry] of this.entries.entries()) if (!entry.written) {
				entry.written = true;
				if (this.onWrite != null) this.onWrite(index, entry);
				if (entry.callback != null) entry.callback.call(void 0);
			}
		}
	}
	get writableLength() {
		return this.entries.reduce((acc, entry) => {
			return acc + (entry.written && entry.length ? entry.length : 0);
		}, 0);
	}
	get writableHighWaterMark() {
		return this[kHighWaterMark];
	}
	get writableCorked() {
		return this[kCorked];
	}
};
/**
* This is an implementation of OutgoingMessage from Node.js intended to run in
* a WinterTC runtime. The 'Writable' interface of this class is wired to an in-memory
* buffer.
*
* This instance can be used in normal ways, but it does not give access to the
* underlying socket (because there isn't one. req.socket will always return null).
*
* Some code in this class is transplanted/adapted from node/lib/_http_outgoing.js
*/
var FetchOutgoingMessage = class extends Writable {
	req;
	outputData;
	outputSize;
	writtenHeaderBytes = 0;
	_last;
	chunkedEncoding;
	shouldKeepAlive;
	maxRequestsOnConnectionReached;
	_defaultKeepAlive;
	useChunkedEncodingByDefault;
	sendDate;
	_removedConnection;
	_removedContLen;
	_removedTE;
	strictContentLength;
	[kBytesWritten];
	_contentLength;
	_hasBody;
	_trailer;
	[kNeedDrain];
	finished;
	_headerSent;
	[kCorked];
	[kChunkedBuffer];
	[kChunkedLength];
	_closed;
	_header;
	[kOutHeaders];
	_keepAliveTimeout;
	_maxRequestsPerSocket;
	_onPendingData;
	[kUniqueHeaders];
	[kErrored];
	[kHighWaterMark];
	[kRejectNonStandardBodyWrites];
	_writtenDataBuffer = new WrittenDataBuffer({ onWrite: this._onDataWritten.bind(this) });
	constructor(req, options) {
		super();
		this.req = req;
		this.outputData = [];
		this.outputSize = 0;
		this.destroyed = false;
		this._last = false;
		this.chunkedEncoding = false;
		this.shouldKeepAlive = true;
		this.maxRequestsOnConnectionReached = false;
		this._defaultKeepAlive = true;
		this.useChunkedEncodingByDefault = true;
		this.sendDate = false;
		this._removedConnection = false;
		this._removedContLen = false;
		this._removedTE = false;
		this.strictContentLength = false;
		this[kBytesWritten] = 0;
		this._contentLength = null;
		this._hasBody = true;
		this._trailer = "";
		this[kNeedDrain] = false;
		this.finished = false;
		this._headerSent = false;
		this[kCorked] = 0;
		this[kChunkedBuffer] = [];
		this[kChunkedLength] = 0;
		this._closed = false;
		this._header = null;
		this[kOutHeaders] = null;
		this._keepAliveTimeout = 0;
		this._onPendingData = nop;
		this[kErrored] = null;
		this[kHighWaterMark] = options?.highWaterMark ?? getDefaultHighWaterMark();
		this[kRejectNonStandardBodyWrites] = options?.rejectNonStandardBodyWrites ?? false;
		this[kUniqueHeaders] = null;
	}
	_renderHeaders() {
		if (this._header) throw new ERR_HTTP_HEADERS_SENT("render");
		const headersMap = this[kOutHeaders];
		const headers = {};
		if (headersMap !== null) {
			const keys = Object.keys(headersMap);
			for (let i = 0, l = keys.length; i < l; i++) {
				const key = keys[i];
				headers[headersMap[key][0]] = headersMap[key][1];
			}
		}
		return headers;
	}
	cork() {
		this[kCorked]++;
		if (this._writtenDataBuffer != null) this._writtenDataBuffer.cork();
	}
	uncork() {
		this[kCorked]--;
		if (this._writtenDataBuffer != null) this._writtenDataBuffer.uncork();
		if (this[kCorked] || this[kChunkedBuffer].length === 0) return;
		const buf = this[kChunkedBuffer];
		for (const { data, encoding, callback } of buf) this._send(data ?? "", encoding, callback);
		this[kChunkedBuffer].length = 0;
		this[kChunkedLength] = 0;
	}
	setTimeout(msecs, callback) {
		return this;
	}
	destroy(error) {
		if (this.destroyed) return this;
		this.destroyed = true;
		this[kErrored] = error;
		return this;
	}
	_send(data, encoding, callback, byteLength) {
		if (!this._headerSent) {
			const header = this._header;
			if (typeof data === "string" && (encoding === "utf8" || encoding === "latin1" || !encoding)) data = header + data;
			else {
				this.outputData.unshift({
					data: header,
					encoding: "latin1",
					callback: void 0
				});
				this.outputSize += header.length;
				this._onPendingData(header.length);
			}
			this._headerSent = true;
			this.writtenHeaderBytes = header.length;
			const [statusLine, ...headerLines] = this._header.split("\r\n");
			const statusLineResult = /^HTTP\/1\.1 (?<statusCode>\d+) (?<statusMessage>.*)$/.exec(statusLine);
			if (statusLineResult == null) throw new Error("Unexpected! Status line was " + statusLine);
			const { statusCode: statusCodeText, statusMessage } = statusLineResult.groups ?? {};
			const statusCode = parseInt(statusCodeText, 10);
			const headers = [];
			for (const headerLine of headerLines) if (headerLine !== "") {
				const pos = headerLine.indexOf(": ");
				const k = headerLine.slice(0, pos);
				const v = headerLine.slice(pos + 2);
				headers.push([k, v]);
			}
			const event = {
				statusCode,
				statusMessage,
				headers
			};
			this.emit("_headersSent", event);
		}
		return this._writeRaw(data, encoding, callback, byteLength);
	}
	_writeRaw(data, encoding, callback, size) {
		if (typeof encoding === "function") {
			callback = encoding;
			encoding = null;
		}
		if (this._writtenDataBuffer != null) {
			if (this.outputData.length) this._flushOutput(this._writtenDataBuffer);
			return this._writtenDataBuffer.write(data, encoding, callback);
		}
		this.outputData.push({
			data,
			encoding,
			callback
		});
		this.outputSize += data.length;
		this._onPendingData(data.length);
		return this.outputSize < this[kHighWaterMark];
	}
	_onDataWritten(index, entry) {
		const event = {
			index,
			entry
		};
		this.emit("_dataWritten", event);
	}
	_storeHeader(firstLine, headers) {
		const state = {
			connection: false,
			contLen: false,
			te: false,
			date: false,
			expect: false,
			trailer: false,
			header: firstLine
		};
		if (headers) {
			if (headers === this[kOutHeaders]) for (const key in headers) {
				const entry = headers[key];
				processHeader(this, state, entry[0], entry[1], false);
			}
			else if (Array.isArray(headers)) if (headers.length && Array.isArray(headers[0])) for (let i = 0; i < headers.length; i++) {
				const entry = headers[i];
				processHeader(this, state, entry[0], entry[1], true);
			}
			else {
				if (headers.length % 2 !== 0) throw new ERR_INVALID_ARG_VALUE("headers", headers);
				for (let n = 0; n < headers.length; n += 2) processHeader(this, state, headers[n], headers[n + 1], true);
			}
			else for (const key in headers) if (headers.hasOwnProperty(key)) {
				const _headers = headers;
				processHeader(this, state, key, _headers[key], true);
			}
		}
		let { header } = state;
		if (this.sendDate && !state.date) header += "Date: " + utcDate() + "\r\n";
		if (this.chunkedEncoding && (this.statusCode === 204 || this.statusCode === 304)) {
			this.chunkedEncoding = false;
			this.shouldKeepAlive = false;
		}
		if (this._removedConnection) this._last = !this.shouldKeepAlive;
		else if (!state.connection) {
			const shouldSendKeepAlive = this.shouldKeepAlive && (state.contLen || this.useChunkedEncodingByDefault);
			if (shouldSendKeepAlive && this.maxRequestsOnConnectionReached) header += "Connection: close\r\n";
			else if (shouldSendKeepAlive) {
				header += "Connection: keep-alive\r\n";
				if (this._keepAliveTimeout && this._defaultKeepAlive) {
					const timeoutSeconds = Math.floor(this._keepAliveTimeout / 1e3);
					let max = "";
					if (this._maxRequestsPerSocket && ~~this._maxRequestsPerSocket > 0) max = `, max=${this._maxRequestsPerSocket}`;
					header += `Keep-Alive: timeout=${timeoutSeconds}${max}\r\n`;
				}
			} else {
				this._last = true;
				header += "Connection: close\r\n";
			}
		}
		if (!state.contLen && !state.te) if (!this._hasBody) this.chunkedEncoding = false;
		else if (!this.useChunkedEncodingByDefault) this._last = true;
		else if (!state.trailer && !this._removedContLen && typeof this._contentLength === "number") header += "Content-Length: " + this._contentLength + "\r\n";
		else if (!this._removedTE) {
			header += "Transfer-Encoding: chunked\r\n";
			this.chunkedEncoding = true;
		} else this._last = true;
		if (this.chunkedEncoding !== true && state.trailer) throw new ERR_HTTP_TRAILER_INVALID();
		this._header = header + "\r\n";
		this._headerSent = false;
		if (state.expect) this._send("");
	}
	get _headers() {
		console.warn("DEP0066: OutgoingMessage.prototype._headers is deprecated");
		return this.getHeaders();
	}
	set _headers(val) {
		console.warn("DEP0066: OutgoingMessage.prototype._headers is deprecated");
		if (val == null) this[kOutHeaders] = null;
		else if (typeof val === "object") {
			const headers = this[kOutHeaders] = Object.create(null);
			const keys = Object.keys(val);
			for (let i = 0; i < keys.length; ++i) {
				const name = keys[i];
				headers[name.toLowerCase()] = [name, val[name]];
			}
		}
	}
	get connection() {
		return null;
	}
	set connection(_socket) {
		console.error("No support for OutgoingMessage.connection");
	}
	get socket() {
		return null;
	}
	set socket(_socket) {
		console.error("No support for OutgoingMessage.socket");
	}
	get _headerNames() {
		console.warn("DEP0066: OutgoingMessage.prototype._headerNames is deprecated");
		const headers = this[kOutHeaders];
		if (headers !== null) {
			const out = Object.create(null);
			const keys = Object.keys(headers);
			for (let i = 0; i < keys.length; ++i) {
				const key = keys[i];
				out[key] = headers[key][0];
			}
			return out;
		}
		return null;
	}
	set _headerNames(val) {
		console.warn("DEP0066: OutgoingMessage.prototype._headerNames is deprecated");
		if (typeof val === "object" && val !== null) {
			const headers = this[kOutHeaders];
			if (!headers) return;
			const keys = Object.keys(val);
			for (let i = 0; i < keys.length; ++i) {
				const header = headers[keys[i]];
				if (header) header[0] = val[keys[i]];
			}
		}
	}
	setHeader(name, value) {
		if (this._header) throw new ERR_HTTP_HEADERS_SENT("set");
		validateHeaderName(name);
		validateHeaderValue(name, value);
		let headers = this[kOutHeaders];
		if (headers === null) this[kOutHeaders] = headers = { __proto__: null };
		headers[name.toLowerCase()] = [name, value];
		return this;
	}
	setHeaders(headers) {
		if (this._header) throw new ERR_HTTP_HEADERS_SENT("set");
		if (!headers || Array.isArray(headers) || typeof headers.keys !== "function" || typeof headers.get !== "function") throw new ERR_INVALID_ARG_TYPE("headers", ["Headers", "Map"], headers);
		const cookies = [];
		for (const { 0: key, 1: value } of headers) {
			if (key === "set-cookie") {
				if (Array.isArray(value)) cookies.push(...value);
				else cookies.push(value);
				continue;
			}
			this.setHeader(key, value);
		}
		if (cookies.length) this.setHeader("set-cookie", cookies);
		return this;
	}
	appendHeader(name, value) {
		if (this._header) throw new ERR_HTTP_HEADERS_SENT("append");
		validateHeaderName(name);
		validateHeaderValue(name, value);
		const field = name.toLowerCase();
		const headers = this[kOutHeaders];
		if (headers === null || !headers[field]) return this.setHeader(name, value);
		if (!Array.isArray(headers[field][1])) headers[field][1] = [headers[field][1]];
		const existingValues = headers[field][1];
		if (Array.isArray(value)) for (let i = 0, length = value.length; i < length; i++) existingValues.push(value[i]);
		else existingValues.push(value);
		return this;
	}
	getHeader(name) {
		validateString(name, "name");
		const headers = this[kOutHeaders];
		if (headers === null) return;
		return headers[name.toLowerCase()]?.[1];
	}
	getHeaderNames() {
		return this[kOutHeaders] !== null ? Object.keys(this[kOutHeaders]) : [];
	}
	getRawHeaderNames() {
		const headersMap = this[kOutHeaders];
		if (headersMap === null) return [];
		const values = Object.values(headersMap);
		const headers = Array(values.length);
		for (let i = 0, l = values.length; i < l; i++) headers[i] = values[i][0];
		return headers;
	}
	getHeaders() {
		const headers = this[kOutHeaders];
		const ret = { __proto__: null };
		if (headers) {
			const keys = Object.keys(headers);
			for (let i = 0; i < keys.length; ++i) {
				const key = keys[i];
				ret[key] = headers[key][1];
			}
		}
		return ret;
	}
	hasHeader(name) {
		validateString(name, "name");
		return this[kOutHeaders] !== null && !!this[kOutHeaders][name.toLowerCase()];
	}
	removeHeader(name) {
		validateString(name, "name");
		if (this._header) throw new ERR_HTTP_HEADERS_SENT("remove");
		const key = name.toLowerCase();
		switch (key) {
			case "connection":
				this._removedConnection = true;
				break;
			case "content-length":
				this._removedContLen = true;
				break;
			case "transfer-encoding":
				this._removedTE = true;
				break;
			case "date":
				this.sendDate = false;
				break;
		}
		if (this[kOutHeaders] !== null) delete this[kOutHeaders][key];
	}
	_implicitHeader() {
		throw new ERR_METHOD_NOT_IMPLEMENTED("_implicitHeader()");
	}
	get headersSent() {
		return !!this._header;
	}
	write(chunk, encoding, callback) {
		if (typeof encoding === "function") {
			callback = encoding;
			encoding = null;
		}
		const ret = write_(this, chunk, encoding, callback, false);
		if (!ret) this[kNeedDrain] = true;
		return ret;
	}
	addTrailers(headers) {
		this._trailer = "";
		const isArray = Array.isArray(headers);
		const keys = isArray ? [...headers.keys()] : Object.keys(headers);
		for (let i = 0, l = keys.length; i < l; i++) {
			let field, value;
			if (isArray) {
				const _headers = headers;
				const key = keys[i];
				field = _headers[key][0];
				value = _headers[key][1];
			} else {
				const _headers = headers;
				const key = keys[i];
				field = key;
				value = _headers[key];
			}
			validateHeaderName(field, "Trailer name");
			if (Array.isArray(value) && value.length > 1 && (!this[kUniqueHeaders] || !this[kUniqueHeaders].has(field.toLowerCase()))) for (let j = 0, l = value.length; j < l; j++) {
				if (checkInvalidHeaderChar$1(value[j])) {
					throw new ERR_INVALID_CHAR("trailer content", field);
				}
				this._trailer += field + ": " + value[j] + "\r\n";
			}
			else {
				if (Array.isArray(value)) value = value.join("; ");
				if (checkInvalidHeaderChar$1(String(value))) {
					throw new ERR_INVALID_CHAR("trailer content", field);
				}
				this._trailer += field + ": " + value + "\r\n";
			}
		}
	}
	end(chunk, encoding, callback) {
		if (typeof chunk === "function") {
			callback = chunk;
			chunk = null;
			encoding = null;
		} else if (typeof encoding === "function") {
			callback = encoding;
			encoding = null;
		}
		if (chunk) {
			if (this.finished) {
				onError(this, new ERR_STREAM_WRITE_AFTER_END(), typeof callback !== "function" ? nop : callback);
				return this;
			}
			if (this._writtenDataBuffer != null) this._writtenDataBuffer.cork();
			write_(this, chunk, encoding, null, true);
		} else if (this.finished) {
			if (typeof callback === "function") if (!this.writableFinished) this.on("finish", callback);
			else callback(new ERR_STREAM_ALREADY_FINISHED("end"));
			return this;
		} else if (!this._header) {
			if (this._writtenDataBuffer != null) this._writtenDataBuffer.cork();
			this._contentLength = 0;
			this._implicitHeader();
		}
		if (typeof callback === "function") this.once("finish", callback);
		if (strictContentLength(this) && this[kBytesWritten] !== this._contentLength) throw new ERR_HTTP_CONTENT_LENGTH_MISMATCH(this[kBytesWritten], this._contentLength);
		const finish = onFinish.bind(void 0, this);
		if (this._hasBody && this.chunkedEncoding) this._send("", "latin1", finish);
		else if (!this._headerSent || this.writableLength || chunk) this._send("", "latin1", finish);
		else setTimeout(finish, 0);
		if (this._writtenDataBuffer != null) this._writtenDataBuffer.uncork();
		this[kCorked] = 1;
		this.uncork();
		this.finished = true;
		if (this.outputData.length === 0 && this._writtenDataBuffer != null) this._finish();
		return this;
	}
	_finish() {
		this.emit("prefinish");
	}
	_flush() {
		if (this._writtenDataBuffer != null) {
			const ret = this._flushOutput(this._writtenDataBuffer);
			if (this.finished) this._finish();
			else if (ret && this[kNeedDrain]) {
				this[kNeedDrain] = false;
				this.emit("drain");
			}
		}
	}
	_flushOutput(dataBuffer) {
		while (this[kCorked]) {
			this[kCorked]--;
			dataBuffer.cork();
		}
		const outputLength = this.outputData.length;
		if (outputLength <= 0) return;
		const outputData = this.outputData;
		dataBuffer.cork();
		let ret;
		for (let i = 0; i < outputLength; i++) {
			const { data, encoding, callback } = outputData[i];
			outputData[i].data = null;
			ret = dataBuffer.write(data ?? "", encoding, callback);
		}
		dataBuffer.uncork();
		this.outputData = [];
		this._onPendingData(-this.outputSize);
		this.outputSize = 0;
		return ret;
	}
	flushHeaders() {
		if (!this._header) this._implicitHeader();
		this._send("");
	}
	pipe(destination) {
		this.emit("error", new ERR_STREAM_CANNOT_PIPE());
		return destination;
	}
};
function processHeader(self, state, key, value, validate) {
	if (validate) validateHeaderName(key);
	if (isContentDispositionField(key) && self._contentLength) if (Array.isArray(value)) for (let i = 0; i < value.length; i++) value[i] = String(Buffer$1.from(String(value[i]), "latin1"));
	else value = String(Buffer$1.from(String(value), "latin1"));
	if (Array.isArray(value)) {
		if ((value.length < 2 || !isCookieField(key)) && (!self[kUniqueHeaders] || !self[kUniqueHeaders].has(key.toLowerCase()))) {
			for (let i = 0; i < value.length; i++) storeHeader(self, state, key, value[i], validate);
			return;
		}
		value = value.join("; ");
	}
	storeHeader(self, state, key, String(value), validate);
}
function storeHeader(self, state, key, value, validate) {
	if (validate) validateHeaderValue(key, value);
	state.header += key + ": " + value + "\r\n";
	matchHeader(self, state, key, value);
}
function validateHeaderName(name, label) {
	if (typeof name !== "string" || !name || !checkIsHttpToken(name)) throw new ERR_INVALID_HTTP_TOKEN(label || "Header name", name);
}
function validateHeaderValue(name, value) {
	if (value === void 0) throw new ERR_HTTP_INVALID_HEADER_VALUE(String(value), name);
	if (checkInvalidHeaderChar$1(String(value))) {
		throw new ERR_INVALID_CHAR("header content", name);
	}
}
function matchHeader(self, state, field, value) {
	if (field.length < 4 || field.length > 17) return;
	field = field.toLowerCase();
	switch (field) {
		case "connection":
			state.connection = true;
			self._removedConnection = false;
			if (RE_CONN_CLOSE.exec(value) !== null) self._last = true;
			else self.shouldKeepAlive = true;
			break;
		case "transfer-encoding":
			state.te = true;
			self._removedTE = false;
			if (chunkExpression.exec(value) !== null) self.chunkedEncoding = true;
			break;
		case "content-length":
			state.contLen = true;
			self._contentLength = +value;
			self._removedContLen = false;
			break;
		case "date":
		case "expect":
		case "trailer":
			state[field] = true;
			break;
		case "keep-alive":
			self._defaultKeepAlive = false;
			break;
	}
}
function onError(msg, err, callback) {
	if (msg.destroyed) return;
	setTimeout(emitErrorNt, 0, msg, err, callback);
}
function emitErrorNt(msg, err, callback) {
	callback(err);
	if (typeof msg.emit === "function" && !msg.destroyed) msg.emit("error", err);
}
function strictContentLength(msg) {
	return msg.strictContentLength && msg._contentLength != null && msg._hasBody && !msg._removedContLen && !msg.chunkedEncoding && !msg.hasHeader("transfer-encoding");
}
function write_(msg, chunk, encoding, callback, fromEnd) {
	if (typeof callback !== "function") callback = nop;
	if (chunk === null) throw new ERR_STREAM_NULL_VALUES();
	else if (typeof chunk !== "string" && !isUint8Array(chunk)) throw new ERR_INVALID_ARG_TYPE("chunk", [
		"string",
		"Buffer",
		"Uint8Array"
	], chunk);
	let err = void 0;
	if (msg.finished) err = new ERR_STREAM_WRITE_AFTER_END();
	else if (msg.destroyed) err = new ERR_STREAM_DESTROYED("write");
	if (err) {
		if (!msg.destroyed) onError(msg, err, callback);
		else setTimeout(callback, 0, err);
		return false;
	}
	let len = void 0;
	if (msg.strictContentLength) {
		len ??= typeof chunk === "string" ? Buffer$1.byteLength(chunk, encoding ?? void 0) : chunk.byteLength;
		if (strictContentLength(msg) && (fromEnd ? msg[kBytesWritten] + len !== msg._contentLength : msg[kBytesWritten] + len > (msg._contentLength ?? 0))) throw new ERR_HTTP_CONTENT_LENGTH_MISMATCH(len + msg[kBytesWritten], msg._contentLength);
		msg[kBytesWritten] += len;
	}
	if (!msg._header) {
		if (fromEnd) {
			len ??= typeof chunk === "string" ? Buffer$1.byteLength(chunk, encoding ?? void 0) : chunk.byteLength;
			msg._contentLength = len;
		}
		msg._implicitHeader();
	}
	if (!msg._hasBody) if (msg[kRejectNonStandardBodyWrites]) throw new ERR_HTTP_BODY_NOT_ALLOWED();
	else {
		setTimeout(callback, 0);
		return true;
	}
	if (!fromEnd && msg._writtenDataBuffer != null && !msg._writtenDataBuffer.writableCorked) {
		msg._writtenDataBuffer.cork();
		setTimeout(connectionCorkNT, 0, msg._writtenDataBuffer);
	}
	let ret;
	if (msg.chunkedEncoding && chunk.length !== 0) {
		len ??= typeof chunk === "string" ? Buffer$1.byteLength(chunk, encoding ?? void 0) : chunk.byteLength;
		if (msg[kCorked] && msg._headerSent) {
			msg[kChunkedBuffer].push({
				data: chunk,
				encoding,
				callback
			});
			msg[kChunkedLength] += len;
			ret = msg[kChunkedLength] < msg[kHighWaterMark];
		} else ret = msg._send(chunk, encoding, callback, len);
	} else ret = msg._send(chunk, encoding, callback, len);
	"" + ret;
	return ret;
}
function connectionCorkNT(dataBuffer) {
	dataBuffer.uncork();
}
function onFinish(outmsg) {
	outmsg.emit("finish");
}
Object.defineProperties(FetchOutgoingMessage.prototype, {
	errored: { get() {
		return this[kErrored];
	} },
	closed: { get() {
		return this._closed;
	} },
	writableFinished: { get() {
		return this.finished && this.outputSize === 0 && (this._writtenDataBuffer == null || this._writtenDataBuffer.writableLength === 0);
	} },
	writableObjectMode: { get() {
		return false;
	} },
	writableLength: { get() {
		return this.outputSize + this[kChunkedLength] + (this._writtenDataBuffer != null ? this._writtenDataBuffer.writableLength : 0);
	} },
	writableHighWaterMark: { get() {
		return this._writtenDataBuffer != null ? this._writtenDataBuffer.writableHighWaterMark : this[kHighWaterMark];
	} },
	writableCorked: { get() {
		return this[kCorked];
	} },
	writableEnded: { get() {
		return this.finished;
	} },
	writableNeedDrain: { get() {
		return !this.destroyed && !this.finished && this[kNeedDrain];
	} }
});
//#endregion
//#region ../../../../../setup-pnpm/node_modules/.bin/store/v11/links/@/fetch-to-node/2.1.0/75a085586b95f3a18a440f8655a7cb5c851846ab2c5e6ad2c4ae036e3bbaa05c/node_modules/fetch-to-node/dist/fetch-to-node/http-server.js
const headerCharRegex = /[^\t\x20-\x7e\x80-\xff]/;
/**
* True if val contains an invalid field-vchar
*  field-value    = *( field-content / obs-fold )
*  field-content  = field-vchar [ 1*( SP / HTAB ) field-vchar ]
*  field-vchar    = VCHAR / obs-text
*/
function checkInvalidHeaderChar(val) {
	return headerCharRegex.test(val);
}
const STATUS_CODES$1 = {
	100: "Continue",
	101: "Switching Protocols",
	102: "Processing",
	103: "Early Hints",
	200: "OK",
	201: "Created",
	202: "Accepted",
	203: "Non-Authoritative Information",
	204: "No Content",
	205: "Reset Content",
	206: "Partial Content",
	207: "Multi-Status",
	208: "Already Reported",
	226: "IM Used",
	300: "Multiple Choices",
	301: "Moved Permanently",
	302: "Found",
	303: "See Other",
	304: "Not Modified",
	305: "Use Proxy",
	307: "Temporary Redirect",
	308: "Permanent Redirect",
	400: "Bad Request",
	401: "Unauthorized",
	402: "Payment Required",
	403: "Forbidden",
	404: "Not Found",
	405: "Method Not Allowed",
	406: "Not Acceptable",
	407: "Proxy Authentication Required",
	408: "Request Timeout",
	409: "Conflict",
	410: "Gone",
	411: "Length Required",
	412: "Precondition Failed",
	413: "Payload Too Large",
	414: "URI Too Long",
	415: "Unsupported Media Type",
	416: "Range Not Satisfiable",
	417: "Expectation Failed",
	418: "I'm a Teapot",
	421: "Misdirected Request",
	422: "Unprocessable Entity",
	423: "Locked",
	424: "Failed Dependency",
	425: "Too Early",
	426: "Upgrade Required",
	428: "Precondition Required",
	429: "Too Many Requests",
	431: "Request Header Fields Too Large",
	451: "Unavailable For Legal Reasons",
	500: "Internal Server Error",
	501: "Not Implemented",
	502: "Bad Gateway",
	503: "Service Unavailable",
	504: "Gateway Timeout",
	505: "HTTP Version Not Supported",
	506: "Variant Also Negotiates",
	507: "Insufficient Storage",
	508: "Loop Detected",
	509: "Bandwidth Limit Exceeded",
	510: "Not Extended",
	511: "Network Authentication Required"
};
/**
* This is an implementation of ServerResponse from Node.js intended to run in
* a WinterTC runtime. The 'Writable' interface of this class is wired to an in-memory
* buffer. This class also provides a method that creates a Response object that
* can be handled by the runtime.
*
* This instance can be used in normal ways, but it does not give access to the
* underlying socket (because there isn't one. req.socket will always return null).
*
* Some code in this class is transplanted/adapted from node/lib/_httpserver.js
*
* NOTE: Node.js doesn't really separate the body from headers, the entire "stream"
* contains the headers and the body. So we provide functions that lets us pull
* the headers and body out individually at a later time.
*/
var FetchServerResponse = class FetchServerResponse extends FetchOutgoingMessage {
	static encoder = new TextEncoder();
	statusCode = 200;
	statusMessage;
	_sent100;
	_expect_continue;
	[kOutHeaders] = null;
	constructor(req, options) {
		super(req, options);
		if (req.method === "HEAD") this._hasBody = false;
		this.sendDate = true;
		this._sent100 = false;
		this._expect_continue = false;
		if (req.httpVersionMajor < 1 || req.httpVersionMinor < 1) {
			this.useChunkedEncodingByDefault = chunkExpression.exec(String(req.headers.te)) !== null;
			this.shouldKeepAlive = false;
		}
		this.fetchResponse = new Promise((resolve) => {
			let finished = false;
			this.on("finish", () => {
				finished = true;
			});
			const initialDataChunks = [];
			const initialDataWrittenHandler = (e) => {
				if (finished) return;
				initialDataChunks[e.index] = this.dataFromDataWrittenEvent(e);
			};
			this.on("_dataWritten", initialDataWrittenHandler);
			this.on("_headersSent", (e) => {
				this.off("_dataWritten", initialDataWrittenHandler);
				const { statusCode, statusMessage, headers } = e;
				resolve(this._toFetchResponse(statusCode, statusMessage, headers, initialDataChunks, finished));
			});
		});
	}
	dataFromDataWrittenEvent(e) {
		const { index, entry } = e;
		let { data, encoding } = entry;
		if (index === 0) {
			if (typeof data !== "string") {
				console.error("First chunk should be string, not sure what happened.");
				throw new ERR_INVALID_ARG_TYPE("packet.data", [
					"string",
					"Buffer",
					"Uint8Array"
				], data);
			}
			data = data.slice(this.writtenHeaderBytes);
		}
		if (typeof data === "string") if (encoding === void 0 || encoding === "utf8" || encoding === "utf-8") data = FetchServerResponse.encoder.encode(data);
		else data = Buffer$1.from(data, encoding ?? void 0);
		return data ?? Buffer$1.from([]);
	}
	_finish() {
		super._finish();
	}
	assignSocket(socket) {
		throw new ERR_METHOD_NOT_IMPLEMENTED("assignSocket");
	}
	detachSocket(socket) {
		throw new ERR_METHOD_NOT_IMPLEMENTED("detachSocket");
	}
	writeContinue(callback) {
		this._writeRaw("HTTP/1.1 100 Continue\r\n\r\n", "ascii", callback);
		this._sent100 = true;
	}
	writeProcessing(callback) {
		this._writeRaw("HTTP/1.1 102 Processing\r\n\r\n", "ascii", callback);
	}
	writeEarlyHints(hints, callback) {
		let head = "HTTP/1.1 103 Early Hints\r\n";
		if (hints.link === null || hints.link === void 0) return;
		const link = validateLinkHeaderValue(hints.link);
		if (link.length === 0) return;
		head += "Link: " + link + "\r\n";
		for (const key of Object.keys(hints)) if (key !== "link") head += key + ": " + hints[key] + "\r\n";
		head += "\r\n";
		this._writeRaw(head, "ascii", callback);
	}
	_implicitHeader() {
		this.writeHead(this.statusCode);
	}
	writeHead(statusCode, reason, obj) {
		if (this._header) throw new ERR_HTTP_HEADERS_SENT("write");
		const originalStatusCode = statusCode;
		statusCode |= 0;
		if (statusCode < 100 || statusCode > 999) throw new ERR_HTTP_INVALID_STATUS_CODE(originalStatusCode);
		if (typeof reason === "string") this.statusMessage = reason;
		else {
			this.statusMessage ||= STATUS_CODES$1[statusCode] || "unknown";
			obj ??= reason;
		}
		this.statusCode = statusCode;
		let headers;
		if (this[kOutHeaders]) {
			let k;
			if (Array.isArray(obj)) {
				if (obj.length % 2 !== 0) throw new ERR_INVALID_ARG_VALUE("headers", obj);
				for (let n = 0; n < obj.length; n += 2) {
					k = obj[n + 0];
					this.removeHeader(String(k));
				}
				for (let n = 0; n < obj.length; n += 2) {
					k = obj[n];
					if (k) this.appendHeader(String(k), obj[n + 1]);
				}
			} else if (obj) {
				const keys = Object.keys(obj);
				for (let i = 0; i < keys.length; i++) {
					k = keys[i];
					if (k) this.setHeader(k, obj[k]);
				}
			}
			headers = this[kOutHeaders];
		} else headers = obj;
		if (checkInvalidHeaderChar(this.statusMessage)) throw new ERR_INVALID_CHAR("statusMessage");
		const statusLine = `HTTP/1.1 ${statusCode} ${this.statusMessage}\r\n`;
		if (statusCode === 204 || statusCode === 304 || statusCode >= 100 && statusCode <= 199) this._hasBody = false;
		if (this._expect_continue && !this._sent100) this.shouldKeepAlive = false;
		const convertedHeaders = headers && !Array.isArray(headers) ? headers : headers;
		this._storeHeader(statusLine, convertedHeaders ?? null);
		return this;
	}
	writeHeader = this.writeHead;
	fetchResponse;
	_toFetchResponse(status, statusText, sentHeaders, initialDataChunks, finished) {
		const headers = new Headers();
		for (const [header, value] of sentHeaders) headers.append(header, value);
		const _this = this;
		let body = this._hasBody ? new ReadableStream({ start(controller) {
			for (const dataChunk of initialDataChunks) controller.enqueue(dataChunk);
			if (finished) controller.close();
			else {
				_this.on("finish", () => {
					finished = true;
					controller.close();
				});
				_this.on("_dataWritten", (e) => {
					if (finished) return;
					const data = _this.dataFromDataWrittenEvent(e);
					controller.enqueue(data);
				});
			}
		} }) : null;
		if (body != null && typeof FixedLengthStream !== "undefined") {
			const contentLength = parseInt(headers.get("content-length") ?? "", 10);
			if (contentLength >= 0) body = body.pipeThrough(new FixedLengthStream(contentLength));
		}
		return new Response(body, {
			status,
			statusText,
			headers
		});
	}
};
function toReqRes(req, options) {
	const { createIncomingMessage = () => new FetchIncomingMessage(), createServerResponse = (incoming) => new FetchServerResponse(incoming), ctx } = {};
	const incoming = createIncomingMessage(ctx);
	const serverResponse = createServerResponse(incoming, ctx);
	const reqUrl = new URL(req.url);
	const versionMajor = 1;
	const versionMinor = 1;
	incoming.httpVersionMajor = versionMajor;
	incoming.httpVersionMinor = versionMinor;
	incoming.httpVersion = `${versionMajor}.${versionMinor}`;
	incoming.url = reqUrl.pathname + reqUrl.search;
	incoming.upgrade = false;
	const headers = [];
	for (const [headerName, headerValue] of req.headers) {
		headers.push(headerName);
		headers.push(headerValue);
	}
	incoming._addHeaderLines(headers, headers.length);
	incoming.method = req.method;
	incoming._stream = req.body;
	return {
		req: incoming,
		res: serverResponse
	};
}
function toFetchResponse(res) {
	if (!(res instanceof FetchServerResponse)) throw new Error("toFetchResponse must be called on a ServerResponse generated by toReqRes");
	return res.fetchResponse;
}
//#endregion
//#region ../../server-adapters/hono/dist/index.js
/**
* Propagates a client disconnect to the simulated Node response produced by `toReqRes`.
*
* `fetch-to-node` builds the outgoing body from `res` events but never observes cancellation
* of the stream it hands back. When an MCP Streamable HTTP client drops its session, nothing
* tells `res` that the socket is gone, so the MCP transport keeps its SSE keep-alive timer
* armed. The next keep-alive tick writes into an already-closed stream controller, and because
* that write originates in a timer callback the resulting `ERR_INVALID_STATE` is unhandled and
* takes down the process.
*
* Emitting `close` on `res` is the signal the MCP Node transport listens for: it aborts the
* request's AbortController, which breaks the write loop and tears down the SSE stream,
* clearing the keep-alive timer. No post-disconnect write is ever attempted.
*/
function propagateClientDisconnect(fetchResponse, res) {
	const upstream = fetchResponse.body;
	if (!upstream) return fetchResponse;
	let disconnected = false;
	const disconnect = () => {
		if (disconnected) return;
		disconnected = true;
		try {
			res.emit("close");
		} catch {}
		reader.read().then(function drain({ done }) {
			return done ? void 0 : reader.read().then(drain);
		}, () => {});
	};
	const reader = upstream.getReader();
	const body = new ReadableStream({
		async pull(controller) {
			const { done, value } = await reader.read();
			if (done) {
				controller.close();
				return;
			}
			controller.enqueue(value);
		},
		cancel() {
			disconnect();
		}
	});
	return new Response(body, {
		status: fetchResponse.status,
		statusText: fetchResponse.statusText,
		headers: fetchResponse.headers
	});
}
/**
* Set up WebSocket-based browser stream endpoint for real-time screencast viewing.
*
* Creates a WebSocket route at `/browser/:agentId/stream` that:
* - Accepts viewer connections
* - Starts screencast when first viewer connects
* - Broadcasts frames to all connected viewers
* - Stops screencast when last viewer disconnects
*
* **Note**: Requires `ws` package to be installed. If not available, returns null
* and logs a warning. Browser streaming will be disabled but everything else works.
*
* @param app - The Hono application instance
* @param config - Configuration for browser stream
* @returns Object containing injectWebSocket function and registry instance, or null if ws is not available
*
* @example
* ```typescript
* import { Hono } from 'hono';
* import { serve } from '@hono/node-server';
* import { setupBrowserStream } from '@mastra/hono';
*
* const app = new Hono();
* const browserStream = await setupBrowserStream(app, {
*   getToolset: (agentId) => browserToolsets.get(agentId),
* });
*
* const server = serve({ fetch: app.fetch, port: 4111 });
* browserStream?.injectWebSocket(server);
* ```
*/
async function setupBrowserStream(app, config) {
	let createNodeWebSocket;
	try {
		createNodeWebSocket = (await import(
			/* @vite-ignore */
			/* webpackIgnore: true */
			'./index2.mjs'
)).createNodeWebSocket;
	} catch {
		return null;
	}
	const { injectWebSocket, upgradeWebSocket } = createNodeWebSocket({ app });
	const registry = new ViewerRegistry();
	const rawPrefix = config.apiPrefix ?? "/api";
	const apiPrefix = (rawPrefix.endsWith("/") ? rawPrefix.slice(0, -1) : rawPrefix) || "/api";
	app.get("/browser/:agentId/stream", upgradeWebSocket((c) => {
		const agentId = c.req.param("agentId");
		const threadId = c.req.query("threadId");
		const viewerKey = threadId ? `${agentId}:${threadId}` : agentId;
		return {
			onOpen(_event, ws) {
				ws.send(JSON.stringify({ status: "connected" }));
				registry.addViewer(viewerKey, ws, config.getToolset, agentId, threadId);
			},
			onMessage(event, _ws) {
				const data = typeof event.data === "string" ? event.data : null;
				if (data) handleInputMessage(data, config.getToolset, agentId, threadId);
			},
			onClose(_event, ws) {
				registry.removeViewer(viewerKey, ws);
			},
			onError(event, ws) {
				console.error("[BrowserStream] WebSocket error:", event);
				registry.removeViewer(viewerKey, ws);
			}
		};
	}));
	app.get(`${apiPrefix}/agents/:agentId/browser/session`, async (c) => {
		const agentId = c.req.param("agentId");
		if (!agentId) return c.json({ error: "Agent ID is required" }, 400);
		const threadId = c.req.query("threadId");
		const toolset = await config.getToolset(agentId);
		if (!toolset) return c.json({
			hasSession: false,
			screencastAvailable: true
		});
		const hasSession = threadId ? toolset.hasThreadSession(threadId) : false;
		return c.json({
			hasSession,
			screencastAvailable: true
		});
	});
	app.post(`${apiPrefix}/agents/:agentId/browser/close`, async (c) => {
		const agentId = c.req.param("agentId");
		if (!agentId) return c.json({ error: "Agent ID is required" }, 400);
		const toolset = await config.getToolset(agentId);
		if (!toolset) return c.json({ error: "No browser session for this agent" }, 404);
		try {
			let threadId;
			try {
				threadId = (await c.req.json())?.threadId;
			} catch {}
			const scope = toolset.getScope();
			const viewerKey = threadId ? `${agentId}:${threadId}` : agentId;
			if (scope === "thread" && threadId) {
				await registry.closeBrowserSession(viewerKey);
				if ("closeThreadSession" in toolset && typeof toolset.closeThreadSession === "function") await toolset.closeThreadSession(threadId);
			} else {
				await registry.closeBrowserSession(viewerKey);
				await toolset.close();
			}
			return c.json({ success: true });
		} catch (error) {
			console.error(`[BrowserStream] Error closing browser for ${agentId}:`, error);
			return c.json({ error: "Failed to close browser" }, 500);
		}
	});
	return {
		injectWebSocket,
		registry
	};
}
let _hasPermissionPromise;
function loadHasPermission() {
	if (!_hasPermissionPromise) _hasPermissionPromise = import('@mastra/core/auth/ee').then((m) => m.hasPermission).catch(() => {
		console.error("[@mastra/hono] Auth features require @mastra/core >= 1.6.0. Please upgrade: npm install @mastra/core@latest");
	});
	return _hasPermissionPromise;
}
/**
* Wrap a Hono middleware handler so it becomes a no-op for framework-public
* routes (routes registered with `requiresAuth: false`).
*
* Adapters that expose user-provided middleware — for example `serverMiddleware`
* on the Mastra instance or `server.middleware` in Mastra config — MUST wrap
* those handlers with this before registering them. This is the framework's
* guarantee that user middleware cannot accidentally (or intentionally) 401
* routes the framework needs to keep reachable (e.g. Studio sign-in endpoints).
*
* The framework-public flag is computed once per request by
* {@link MastraServer.registerContextMiddleware} and stashed on the Hono
* context under `MASTRA_FRAMEWORK_PUBLIC_KEY`.
*/
const skipIfFrameworkPublic = (handler) => {
	return async (c, next) => {
		if (c.get(MASTRA_FRAMEWORK_PUBLIC_KEY)) return next();
		return handler(c, next);
	};
};
/**
* Context key holding a pristine clone of the incoming request, captured by
* the context middleware before user middleware runs. The custom-route bridge
* reads the body from this clone so user middleware that consumes the request
* body (e.g. `await c.req.json()`) does not break custom API routes.
*/
const MASTRA_PRISTINE_REQUEST_KEY = "__mastraPristineRequest";
const BODY_METHODS = /* @__PURE__ */ new Set([
	"POST",
	"PUT",
	"PATCH",
	"DELETE"
]);
var MastraServer$1 = class extends MastraServer {
	createContextMiddleware() {
		return async (c, next) => {
			if (this.hasCustomRouteHandler && BODY_METHODS.has(c.req.method) && c.req.raw.body) c.set(MASTRA_PRISTINE_REQUEST_KEY, c.req.raw.clone());
			const originalJson = c.req.json.bind(c.req);
			let jsonPromise;
			c.req.json = () => {
				if (!jsonPromise) jsonPromise = originalJson().then((body) => {
					c.set("cachedBody", body);
					return body;
				});
				return jsonPromise;
			};
			let bodyRequestContext;
			let paramsRequestContext;
			if (c.req.method === "POST" || c.req.method === "PUT") {
				const contentType = c.req.header("content-type");
				const contentLength = c.req.header("content-length");
				if (contentType?.includes("application/json") && contentLength !== "0") try {
					const body = await c.req.raw.clone().json();
					if (body.requestContext) bodyRequestContext = body.requestContext;
				} catch {}
			}
			if (c.req.method === "GET" || c.req.method === "POST") try {
				const encodedRequestContext = c.req.query("requestContext");
				if (encodedRequestContext) try {
					paramsRequestContext = JSON.parse(encodedRequestContext);
				} catch {
					try {
						const json = Buffer.from(encodedRequestContext, "base64").toString("utf-8");
						paramsRequestContext = JSON.parse(json);
					} catch {}
				}
			} catch {}
			const requestContext = this.mergeRequestContext({
				paramsRequestContext,
				bodyRequestContext
			});
			this.applyRequestMetadataToContext({
				requestContext,
				getHeader: (name) => c.req.header(name)
			});
			c.set("requestContext", requestContext);
			c.set("mastra", this.mastra);
			c.set("registeredTools", this.tools || {});
			c.set("taskStore", this.taskStore);
			c.set("abortSignal", c.req.raw.signal);
			c.set("customRouteAuthConfig", this.customRouteAuthConfig);
			return next();
		};
	}
	async stream(route, res, result) {
		const streamFormat = route.streamFormat || "stream";
		if (streamFormat === "sse") {
			res.header("Content-Type", "text/event-stream");
			res.header("Cache-Control", "no-cache");
			res.header("Connection", "keep-alive");
			res.header("X-Accel-Buffering", "no");
		} else res.header("Content-Type", "text/plain");
		res.header("Transfer-Encoding", "chunked");
		return stream(res, async (stream) => {
			if (streamFormat === "sse" && route.sseFlushOnConnect) await stream.write(": connected\n\n");
			const reader = (result instanceof ReadableStream ? result : result.fullStream).getReader();
			stream.onAbort(() => {
				reader.cancel("request aborted").catch(() => {});
			});
			try {
				while (true) {
					const { done, value } = await reader.read();
					if (done) break;
					if (value) {
						if (streamFormat === "sse" && typeof value === "string" && value.startsWith(":")) {
							await stream.write(value);
							continue;
						}
						const outputValue = this.streamOptions?.redact ?? true ? redactStreamChunk(value) : value;
						const serialized = serializeStreamChunk(outputValue);
						if (!serialized.ok) {
							this.mastra.getLogger()?.error("Failed to serialize stream chunk, skipping", {
								path: route.path,
								chunkType: outputValue?.type,
								error: serialized.error.message
							});
							continue;
						}
						if (streamFormat === "sse") await stream.write(`data: ${serialized.json}\n\n`);
						else await stream.write(serialized.json + "");
					}
				}
				if (streamFormat === "sse") await stream.write("data: [DONE]\n\n");
			} catch (error) {
				this.mastra.getLogger()?.error("Error in stream processing", { error: error instanceof Error ? {
					message: error.message,
					stack: error.stack
				} : error });
			} finally {
				await stream.close();
			}
		}, async (err) => {
			this.mastra.getLogger()?.error("Stream error callback", { error: err instanceof Error ? {
				message: err.message,
				stack: err.stack
			} : err });
		});
	}
	async getParams(route, request) {
		const urlParams = request.param();
		const queryParams = normalizeQueryParams(request.queries());
		let body;
		let bodyParseError;
		if (route.method === "POST" || route.method === "PUT" || route.method === "PATCH" || route.method === "DELETE") {
			const contentType = request.header("content-type") || "";
			if (contentType.includes("multipart/form-data")) try {
				const formData = await request.formData();
				body = await this.parseFormData(formData);
			} catch (error) {
				this.mastra.getLogger()?.error("Failed to parse multipart form data", { error: error instanceof Error ? {
					message: error.message,
					stack: error.stack
				} : error });
				if (error instanceof Error && error.message.toLowerCase().includes("size")) throw error;
				bodyParseError = { message: error instanceof Error ? error.message : "Failed to parse multipart form data" };
			}
			else if (contentType.includes("application/json")) {
				const bodyText = await request.raw.clone().text();
				if (bodyText && bodyText.trim().length > 0) try {
					body = JSON.parse(bodyText);
				} catch (error) {
					this.mastra.getLogger()?.error("Failed to parse JSON body", { error: error instanceof Error ? {
						message: error.message,
						stack: error.stack
					} : error });
					bodyParseError = { message: error instanceof Error ? error.message : "Invalid JSON in request body" };
				}
			}
		}
		return {
			urlParams,
			queryParams,
			body,
			bodyParseError
		};
	}
	/**
	* Parse FormData into a plain object, converting File objects to Buffers.
	*/
	async parseFormData(formData) {
		const result = {};
		for (const [key, value] of formData.entries()) {
			if (!isSafeObjectKey(key)) continue;
			if (value instanceof File) {
				const arrayBuffer = await value.arrayBuffer();
				result[key] = Buffer.from(arrayBuffer);
			} else if (typeof value === "string") try {
				result[key] = JSON.parse(value);
			} catch {
				result[key] = value;
			}
			else result[key] = value;
		}
		return result;
	}
	async sendResponse(route, response, result, prefix) {
		const resolvedPrefix = prefix ?? this.prefix ?? "";
		if (result && typeof result === "object" && "__refreshHeaders" in result) {
			const refreshHeaders = result.__refreshHeaders;
			for (const [key, value] of Object.entries(refreshHeaders)) response.header(key, value);
			delete result.__refreshHeaders;
		}
		if (route.responseType === "json") return response.json(result, 200);
		else if (route.responseType === "stream") return this.stream(route, response, result);
		else if (route.responseType === "datastream-response") return result;
		else if (route.responseType === "mcp-http") {
			const { server, httpPath, mcpOptions: routeMcpOptions } = result;
			const { req, res } = toReqRes(response.req.raw);
			const { setRequestAuth, ...options } = {
				...this.mcpOptions,
				...routeMcpOptions
			};
			await applyMcpRequestAuth({
				req,
				requestContext: response.get("requestContext"),
				setRequestAuth
			});
			server.startHTTP({
				url: new URL(response.req.url),
				httpPath: `${resolvedPrefix}${httpPath}`,
				req,
				res,
				options: Object.keys(options).length > 0 ? options : void 0
			}).catch((e) => {
				this.mastra.getLogger()?.error("[MCP HTTP] Error in background startHTTP:", { error: e instanceof Error ? {
					message: e.message,
					stack: e.stack
				} : e });
				try {
					if (!res.headersSent) {
						res.writeHead(500, { "Content-Type": "application/json" });
						res.end(JSON.stringify({
							jsonrpc: "2.0",
							error: {
								code: -32603,
								message: "Internal server error"
							},
							id: null
						}));
					}
				} catch {}
			});
			return propagateClientDisconnect(await toFetchResponse(res), res);
		} else if (route.responseType === "mcp-sse") {
			const { server, ssePath, messagePath } = result;
			try {
				const { req } = toReqRes(response.req.raw);
				await applyMcpRequestAuth({
					req,
					requestContext: response.get("requestContext"),
					setRequestAuth: this.mcpOptions?.setRequestAuth
				});
				return await server.startHonoSSE({
					url: new URL(response.req.url),
					ssePath: `${resolvedPrefix}${ssePath}`,
					messagePath: `${resolvedPrefix}${messagePath}`,
					context: response,
					authInfo: req.auth
				});
			} catch {
				return response.json({ error: "Error handling MCP SSE request" }, 500);
			}
		} else return response.status(500);
	}
	async registerRoute(app, route, { prefix: prefixParam } = {}) {
		const prefix = prefixParam ?? this.prefix ?? "";
		const maxSize = route.maxBodySize ?? this.bodyLimitOptions?.maxSize;
		const isBodyMethod = [
			"POST",
			"PUT",
			"PATCH",
			"DELETE"
		].includes(route.method.toUpperCase());
		const middlewares = [];
		if (isBodyMethod && maxSize !== void 0) middlewares.push(bodyLimit({
			maxSize,
			onError: (c) => {
				let errorResponse = { error: "Request body too large" };
				if (route.maxBodySize === void 0 && this.bodyLimitOptions) try {
					errorResponse = this.bodyLimitOptions.onError(errorResponse);
				} catch {}
				return c.json(errorResponse, 413);
			}
		}));
		app[route.method.toLowerCase()](`${prefix}${route.path}`, ...middlewares, async (c) => {
			const authResult = await this.checkRouteAuth(route, {
				path: c.req.path,
				method: c.req.method,
				getHeader: (name) => c.req.header(name),
				getQuery: (name) => c.req.query(name),
				requestContext: c.get("requestContext"),
				request: c.req.raw,
				buildAuthorizeContext: () => c
			});
			if (authResult) {
				if (authResult.headers) for (const [key, value] of Object.entries(authResult.headers)) c.header(key, value);
				if (authResult.error) return c.json({ error: authResult.error }, authResult.status);
			}
			const params = await this.getParams(route, c.req);
			if (params.bodyParseError) return c.json({
				error: "Invalid request body",
				issues: [{
					field: "body",
					message: params.bodyParseError.message
				}]
			}, 400);
			if (params.queryParams) try {
				params.queryParams = await this.parseQueryParams(route, params.queryParams);
			} catch (error) {
				this.mastra.getLogger()?.error("Error parsing query params", { error: error instanceof Error ? {
					message: error.message,
					stack: error.stack
				} : error });
				if (isZodError(error)) {
					const { status, body } = this.resolveValidationError(route, error, "query");
					return c.json(body, status);
				}
				return c.json({
					error: "Invalid query parameters",
					issues: [{
						field: "unknown",
						message: error instanceof Error ? error.message : "Unknown error"
					}]
				}, 400);
			}
			if (params.body !== void 0 || route.bodySchema) try {
				params.body = await this.parseBody(route, params.body);
			} catch (error) {
				this.mastra.getLogger()?.error("Error parsing body", { error: error instanceof Error ? {
					message: error.message,
					stack: error.stack
				} : error });
				if (isZodError(error)) {
					const { status, body } = this.resolveValidationError(route, error, "body");
					return c.json(body, status);
				}
				return c.json({
					error: "Invalid request body",
					issues: [{
						field: "unknown",
						message: error instanceof Error ? error.message : "Unknown error"
					}]
				}, 400);
			}
			if (params.urlParams) try {
				params.urlParams = await this.parsePathParams(route, params.urlParams);
			} catch (error) {
				this.mastra.getLogger()?.error("Error parsing path params", { error: error instanceof Error ? {
					message: error.message,
					stack: error.stack
				} : error });
				if (isZodError(error)) {
					const { status, body } = this.resolveValidationError(route, error, "path");
					return c.json(body, status);
				}
				return c.json({
					error: "Invalid path parameters",
					issues: [{
						field: "unknown",
						message: error instanceof Error ? error.message : "Unknown error"
					}]
				}, 400);
			}
			const handlerParams = {
				...params.urlParams,
				...params.queryParams,
				...typeof params.body === "object" ? params.body : {},
				requestContext: c.get("requestContext"),
				mastra: this.mastra,
				registeredTools: c.get("registeredTools"),
				taskStore: c.get("taskStore"),
				abortSignal: c.get("abortSignal"),
				routePrefix: prefix,
				request: c.req.raw
			};
			const requestContext = c.get("requestContext");
			if (this.mastra.getStudio?.()?.auth || this.mastra.getServer()?.auth) {
				const hasPermission = await loadHasPermission();
				if (hasPermission) {
					const userPermissions = requestContext.get("mastra__userPermissions");
					const permissionError = this.checkRoutePermission(route, userPermissions, hasPermission, requestContext);
					if (permissionError) return c.json({
						error: permissionError.error,
						message: permissionError.message
					}, permissionError.status);
				}
			}
			const fgaError = await checkRouteFGA(this.mastra, route, c.get("requestContext"), {
				...params.urlParams,
				...params.queryParams,
				...typeof params.body === "object" ? params.body : {}
			});
			if (fgaError) return c.json({
				error: fgaError.error,
				message: fgaError.message
			}, fgaError.status);
			try {
				const result = await route.handler(handlerParams);
				return this.sendResponse(route, c, result, prefix);
			} catch (error) {
				const httpStatus = error && typeof error === "object" ? error.status ?? error.details?.status : void 0;
				if (!(typeof httpStatus === "number" && httpStatus >= 400 && httpStatus < 500)) {
					const logLevel = httpStatus === 501 ? "warn" : "error";
					this.mastra.getLogger()?.[logLevel]("Error calling handler", {
						error: error instanceof Error ? {
							message: error.message,
							stack: error.stack
						} : error,
						path: route.path,
						method: route.method
					});
				}
				const customResponse = getCustomHTTPExceptionResponse(error);
				if (customResponse) return customResponse;
				if (error && typeof error === "object") {
					if ("status" in error) {
						const status = error.status;
						let safeCause;
						try {
							const raw = error instanceof Error ? error.cause : void 0;
							if (raw && typeof raw === "object" && !Array.isArray(raw) && "failingItems" in raw && Array.isArray(raw.failingItems)) safeCause = { failingItems: raw.failingItems };
						} catch {}
						return c.json({
							error: error instanceof Error ? error.message : "Unknown error",
							...safeCause ? { cause: safeCause } : {}
						}, status);
					}
					if ("details" in error && error.details && typeof error.details === "object" && "status" in error.details) {
						const status = error.details.status;
						return c.json({ error: error instanceof Error ? error.message : "Unknown error" }, status);
					}
				}
				return c.json({ error: error instanceof Error ? error.message : "Unknown error" }, 500);
			}
		});
	}
	async registerCustomApiRoutes() {
		const routes = await this.registerSchemaApiRoutes();
		if (!await this.buildCustomRouteHandler(routes)) return;
		for (const route of routes) {
			const serverRoute = {
				method: route.method,
				path: route.path,
				responseType: "json",
				handler: async () => {},
				requiresAuth: route.requiresAuth,
				requiresPermission: route.requiresPermission,
				fga: route.fga
			};
			const routeHandler = async (c) => {
				const authError = await this.checkRouteAuth(serverRoute, {
					path: c.req.path,
					method: c.req.method,
					getHeader: (name) => c.req.header(name),
					getQuery: (name) => c.req.query(name),
					requestContext: c.get("requestContext"),
					request: c.req.raw,
					buildAuthorizeContext: () => c
				});
				if (authError) {
					if (authError.headers) for (const [key, value] of Object.entries(authError.headers)) c.header(key, value);
					if (authError.error) return c.json({ error: authError.error }, authError.status);
				}
				const requestContext = c.get("requestContext");
				if (this.mastra.getStudio?.()?.auth || this.mastra.getServer()?.auth) {
					const hasPermission = await loadHasPermission();
					if (hasPermission) {
						const userPermissions = requestContext.get("mastra__userPermissions");
						const permissionError = this.checkRoutePermission(serverRoute, userPermissions, hasPermission, requestContext);
						if (permissionError) return c.json({
							error: permissionError.error,
							message: permissionError.message
						}, permissionError.status);
					}
				}
				const pristineRequest = c.get(MASTRA_PRISTINE_REQUEST_KEY) ?? c.req.raw;
				let bodyParams = {};
				if (getFGAProvider(this.mastra, c.get("requestContext"))) {
					const contentType = c.req.header("content-type");
					if (contentType?.includes("application/json")) try {
						const body = await pristineRequest.clone().json();
						if (body && typeof body === "object" && !Array.isArray(body)) bodyParams = body;
					} catch {
						bodyParams = {};
					}
					else if (contentType?.includes("application/x-www-form-urlencoded") || contentType?.includes("multipart/form-data")) try {
						bodyParams = Object.fromEntries(await pristineRequest.clone().formData());
					} catch {
						bodyParams = {};
					}
				}
				const fgaError = await checkRouteFGA(this.mastra, serverRoute, c.get("requestContext"), {
					...c.req.param(),
					...Object.fromEntries(new URL(c.req.url).searchParams.entries()),
					...bodyParams
				});
				if (fgaError) return c.json({
					error: fgaError.error,
					message: fgaError.message
				}, fgaError.status);
				const reqHeaders = {};
				c.req.raw.headers.forEach((v, k) => {
					reqHeaders[k] = v;
				});
				let executionCtx;
				try {
					executionCtx = c.executionCtx;
				} catch {
					executionCtx = void 0;
				}
				const response = await this.handleCustomRouteRequest(c.req.url, c.req.method, reqHeaders, pristineRequest.body, c.get("requestContext"), c.req.raw.signal, executionCtx);
				if (!response) return c.json({ error: "Not Found" }, 404);
				return response;
			};
			const method = route.method.toLowerCase();
			this.app[method](route.path, routeHandler);
		}
	}
	registerContextMiddleware() {
		const isFrameworkPublic = this.getFrameworkPublicMatcher();
		this.app.use("*", this.createContextMiddleware());
		this.app.use("*", async (c, next) => {
			c.set(MASTRA_FRAMEWORK_PUBLIC_KEY, isFrameworkPublic(c.req.path, c.req.method));
			return next();
		});
		this.app.use("*", async (c, next) => {
			await next();
			this.warnIfUnregisteredChannelWebhook(c.req.path, c.req.method, c.res.status);
		});
	}
	registerAuthMiddleware() {}
	registerUserMiddleware() {
		for (const m of this.mastra.getServerMiddleware?.() ?? []) this.app.use(m.path, skipIfFrameworkPublic(m.handler));
		const configMiddleware = this.mastra.getServer()?.middleware;
		if (!configMiddleware) return;
		const normalizedMiddlewares = Array.isArray(configMiddleware) ? configMiddleware : [configMiddleware];
		for (const middleware of normalizedMiddlewares) {
			const { path, handler } = typeof middleware === "function" ? {
				path: "*",
				handler: middleware
			} : middleware;
			this.app.use(path, skipIfFrameworkPublic(handler));
		}
	}
	registerHttpLoggingMiddleware() {
		if (!this.httpLoggingConfig?.enabled) return;
		this.app.use("*", async (c, next) => {
			if (!this.shouldLogRequest(c.req.path)) return next();
			const start = Date.now();
			const method = c.req.method;
			const path = c.req.path;
			await next();
			const duration = Date.now() - start;
			const status = c.res.status;
			const level = this.httpLoggingConfig?.level || "info";
			const logData = {
				method,
				path,
				status,
				duration: `${duration}ms`
			};
			if (this.httpLoggingConfig?.includeQueryParams) logData.query = c.req.query();
			if (this.httpLoggingConfig?.includeHeaders) {
				const headers = Object.fromEntries(c.req.raw.headers.entries());
				(this.httpLoggingConfig.redactHeaders || []).forEach((h) => {
					const key = h.toLowerCase();
					if (headers[key] !== void 0) headers[key] = "[REDACTED]";
				});
				logData.headers = headers;
			}
			this.logger[level](`${method} ${path} ${status} ${duration}ms`, logData);
		});
	}
};
//#endregion
//#region ../../../../../setup-pnpm/node_modules/.bin/store/v11/links/@/hono-openapi/1.3.1/2557b2465f29e88b77390131b99b4ae79c844dd5b524b41cc78f5b3bfa1fdf70/node_modules/hono-openapi/dist/index.js
const uniqueSymbol = Symbol("openapi");
function describeRoute(spec) {
	const middleware = async (_c, next) => {
		await next();
	};
	Object.defineProperty(middleware, "name", {
		value: "describeRoute",
		configurable: true
	});
	return Object.assign(middleware, { [uniqueSymbol]: { spec } });
}
//#endregion
//#region src/server/handlers/agent-learning.ts
const DEFAULT_AGENT_LEARNING_ENDPOINT = "https://output.signals.mastra.ai";
const AGENT_LEARNING_TIMEOUT_MS = 8e3;
const LOOPBACK_HOSTS = /* @__PURE__ */ new Set([
	"localhost",
	"127.0.0.1",
	"[::1]"
]);
async function agentLearningProxyHandler(c) {
	const requestUrl = new URL(c.req.url);
	if (!LOOPBACK_HOSTS.has(requestUrl.hostname)) return c.json({ error: "Agent Learning development proxy is only available on loopback hosts" }, 403);
	const origin = c.req.header("Origin");
	if (origin && origin !== requestUrl.origin) return c.json({ error: "Cross-origin Agent Learning requests are not allowed" }, 403);
	const accessToken = process$1.env.MASTRA_PLATFORM_ACCESS_TOKEN;
	const projectId = process$1.env.MASTRA_PROJECT_ID;
	if (!accessToken || !projectId) return c.json({ error: "MASTRA_PLATFORM_ACCESS_TOKEN and MASTRA_PROJECT_ID are required for Agent Learning" }, 503);
	const endpoint = process$1.env.MASTRA_PLATFORM_AGENT_LEARNING_ENDPOINT || DEFAULT_AGENT_LEARNING_ENDPOINT;
	let upstreamUrl;
	try {
		upstreamUrl = new URL(`${requestUrl.pathname}${requestUrl.search}`, `${endpoint.replace(/\/+$/, "")}/`);
	} catch {
		return c.json({ error: "MASTRA_PLATFORM_AGENT_LEARNING_ENDPOINT must be a valid URL" }, 503);
	}
	try {
		const upstream = await fetch(upstreamUrl, {
			method: "GET",
			headers: {
				Accept: c.req.header("Accept") || "application/json",
				Authorization: `Bearer ${accessToken}`,
				"X-Mastra-Project-Id": projectId
			},
			signal: AbortSignal.timeout(AGENT_LEARNING_TIMEOUT_MS)
		});
		const headers = new Headers();
		const contentType = upstream.headers.get("content-type");
		const cacheControl = upstream.headers.get("cache-control");
		if (contentType) headers.set("content-type", contentType);
		if (cacheControl) headers.set("cache-control", cacheControl);
		return new Response(upstream.body, {
			status: upstream.status,
			statusText: upstream.statusText,
			headers
		});
	} catch {
		return c.json({ error: "Agent Learning upstream unavailable" }, 502);
	}
}
//#endregion
//#region src/server/handlers/client.ts
const clients = /* @__PURE__ */ new Set();
let hotReloadDisabled = false;
function handleClientsRefreshRequest(abortSignal, devServerInstanceId) {
	const stream = new ReadableStream({ start(controller) {
		clients.add(controller);
		controller.enqueue(`${devServerInstanceId ? `id: ${devServerInstanceId}\n` : ""}data: connected\n\n`);
		abortSignal.addEventListener("abort", () => {
			clients.delete(controller);
		});
	} });
	return new Response(stream, { headers: {
		"Content-Type": "text/event-stream",
		"Cache-Control": "no-cache",
		Connection: "keep-alive"
	} });
}
/**
* End every open refresh SSE stream. Studio keeps one of these connections
* open for its whole lifetime, so without this the graceful-shutdown drain
* would wait on them until the drain deadline (stalling `mastra dev` hot
* reloads while Studio is open).
*/
function closeRefreshStreams() {
	clients.forEach((controller) => {
		try {
			controller.close();
		} catch {}
	});
	clients.clear();
}
function getTriggerClientsRefreshPayload() {
	clients.forEach((controller) => {
		try {
			controller.enqueue("data: refresh\n\n");
		} catch {
			clients.delete(controller);
		}
	});
	return {
		success: true,
		clients: clients.size
	};
}
function isHotReloadDisabled() {
	return hotReloadDisabled;
}
//#endregion
//#region src/server/handlers/error.ts
function handleError(error, defaultMessage) {
	const apiError = error;
	throw new HTTPException(apiError.status || 500, {
		message: apiError.message || defaultMessage,
		cause: apiError.cause
	});
}
function errorHandler(err, c, isDev) {
	if (err instanceof HTTPException) {
		if (isDev) return c.json({
			error: err.message,
			cause: err.cause,
			stack: err.stack
		}, err.status);
		return c.json({ error: err.message }, err.status);
	}
	c.get("mastra").getLogger().error(err);
	return c.json({ error: "Internal Server Error" }, 500);
}
//#endregion
//#region src/server/handlers/health.ts
async function healthHandler(c) {
	return c.json({ success: true }, 200);
}
//#endregion
//#region src/server/handlers/restart-active-runs.ts
async function restartAllActiveWorkflowRunsHandler(c) {
	try {
		const mastra = c.get("mastra");
		if (mastra.recoveryConfig?.workflows !== "off") mastra.restartAllActiveWorkflowRuns().catch((error) => {
			mastra.getLogger().error("Failed to restart active workflow runs during server startup", { error });
		});
		if (mastra.recoveryConfig?.durableAgents === "auto") mastra.recoverAllDurableAgents().catch((error) => {
			mastra.getLogger().error("Failed to recover durable agent runs during server startup", { error });
		});
		return c.json({ message: "Restarting all active workflow runs..." });
	} catch (error) {
		return handleError(error, "Error restarting active workflow runs");
	}
}
//#endregion
//#region src/server/handlers/root.ts
async function rootHandler(c) {
	const baseUrl = new URL(c.req.url).origin;
	return c.html(`
    <!doctype html>
    <html lang="en">
      <head>
        <meta charset="UTF-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1.0" />
        <title>Mastra API</title>
        <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/inter-ui/3.19.3/inter.min.css" />
        <style>
          body {
            margin: 0;
            padding: 0;
            background-color: #0d0d0d;
            color: #ffffff;
            font-family:
              'Inter',
              -apple-system,
              BlinkMacSystemFont,
              system-ui,
              sans-serif;
            min-height: 100vh;
            display: flex;
            flex-direction: column;
          }

          main {
            flex: 1;
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            padding: 2rem;
            text-align: center;
          }

          h1 {
            font-size: 4rem;
            font-weight: 600;
            margin: 0 0 1rem 0;
            background: linear-gradient(to right, #fff, #ccc);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            line-height: 1.2;
          }

          .subtitle {
            color: #9ca3af;
            font-size: 1.25rem;
            max-width: 600px;
            margin: 0 auto 3rem auto;
            line-height: 1.6;
          }

          .link {
            color: #ffffff;
          }

          .link:hover {
            text-decoration: none;
          }

          .docs-link {
            background-color: #1a1a1a;
            padding: 1rem 2rem;
            border-radius: 0.5rem;
            display: flex;
            align-items: center;
            gap: 1rem;
            font-family: monospace;
            font-size: 1rem;
            color: #ffffff;
            text-decoration: none;
            transition: background-color 0.2s;
          }

          .docs-link:hover {
            background-color: #252525;
          }

          .arrow-icon {
            transition: transform 0.2s;
          }

          .docs-link:hover .arrow-icon {
            transform: translateX(4px);
          }
        </style>
      </head>
      <body>
        <main>
          <h1>Welcome to the Mastra API</h1>
          <p class="subtitle">
            Discover all available endpoints through Swagger UI. You can also define your own <a href="https://mastra.ai/docs/server-db/custom-api-routes" target="_blank" class="link">API routes</a>.
          </p>
          <a href="${baseUrl}/swagger-ui" target="_blank" class="docs-link">
            Browse Swagger UI
            <svg
              class="arrow-icon"
              width="20"
              height="20"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              strokeWidth="2"
            >
              <path d="M5 12h14M12 5l7 7-7 7" />
            </svg>
          </a>
        </main>
      </body>
    </html>
    `);
}
//#endregion
//#region src/server/welcome.ts
/**
* Trim leading and trailing slashes. Index scan instead of regex to avoid
* backtracking (CodeQL js/polynomial-redos).
*/
function trimSlashes(value) {
	let start = 0;
	let end = value.length;
	while (start < end && value[start] === "/") start++;
	while (end > start && value[end - 1] === "/") end--;
	return value.slice(start, end);
}
function welcomeHtml(apiPrefix = "/api") {
	const prefix = "/" + trimSlashes(apiPrefix);
	const prefixNoSlash = prefix.slice(1);
	return `
<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Mastra Server</title>
    <style>
      * { box-sizing: border-box; }

      body {
        margin: 0;
        padding: 0;
        background-color: #0a0a0a;
        color: #e4e4e7;
        font-family: -apple-system, BlinkMacSystemFont, system-ui, sans-serif;
        min-height: 100vh;
        line-height: 1.6;
      }

      .page {
        max-width: 720px;
        margin: 0 auto;
        padding: 3rem 1.5rem 4rem;
      }

      header {
        margin-bottom: 2.5rem;
      }

      header h1 {
        font-size: 1.75rem;
        font-weight: 600;
        margin: 0 0 0.25rem;
        color: #fff;
      }

      header p {
        color: #71717a;
        font-size: 0.9rem;
        margin: 0;
      }

      .status-bar {
        display: flex;
        align-items: center;
        gap: 0.5rem;
        margin-top: 1rem;
        font-size: 0.8rem;
        color: #a1a1aa;
      }

      .status-dot {
        width: 8px;
        height: 8px;
        border-radius: 50%;
        background: #22c55e;
        flex-shrink: 0;
      }

      section {
        margin-bottom: 2rem;
      }

      section h2 {
        font-size: 0.75rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        color: #71717a;
        margin: 0 0 0.75rem;
      }

      .card {
        background: #18181b;
        border: 1px solid #27272a;
        border-radius: 8px;
        overflow: hidden;
      }

      .card + .card {
        margin-top: 0.75rem;
      }

      .card-row {
        display: flex;
        align-items: center;
        justify-content: space-between;
        padding: 0.75rem 1rem;
        gap: 0.75rem;
      }

      .card-row + .card-row {
        border-top: 1px solid #27272a;
      }

      .card-row-left {
        display: flex;
        align-items: center;
        gap: 0.75rem;
        min-width: 0;
      }

      .method {
        font-family: 'SF Mono', SFMono-Regular, ui-monospace, Menlo, monospace;
        font-size: 0.7rem;
        font-weight: 600;
        padding: 2px 6px;
        border-radius: 4px;
        flex-shrink: 0;
      }

      .method-get { background: #052e16; color: #4ade80; }
      .method-post { background: #172554; color: #60a5fa; }

      .endpoint-path {
        font-family: 'SF Mono', SFMono-Regular, ui-monospace, Menlo, monospace;
        font-size: 0.8rem;
        color: #d4d4d8;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
      }

      .endpoint-desc {
        font-size: 0.75rem;
        color: #71717a;
        flex-shrink: 0;
      }

      .curl-block {
        position: relative;
        background: #111113;
        border: 1px solid #27272a;
        border-radius: 8px;
        overflow: hidden;
      }

      .curl-block + .curl-block {
        margin-top: 0.75rem;
      }

      .curl-label {
        display: flex;
        align-items: center;
        justify-content: space-between;
        padding: 0.5rem 1rem;
        border-bottom: 1px solid #27272a;
        font-size: 0.75rem;
        color: #a1a1aa;
      }

      .copy-btn {
        background: none;
        border: 1px solid #3f3f46;
        border-radius: 4px;
        color: #a1a1aa;
        font-size: 0.7rem;
        padding: 2px 8px;
        cursor: pointer;
        font-family: inherit;
        transition: all 0.15s;
      }

      .copy-btn:hover {
        background: #27272a;
        color: #e4e4e7;
      }

      .curl-code {
        padding: 0.75rem 1rem;
        font-family: 'SF Mono', SFMono-Regular, ui-monospace, Menlo, monospace;
        font-size: 0.8rem;
        line-height: 1.5;
        color: #d4d4d8;
        overflow-x: auto;
        white-space: pre;
        margin: 0;
      }

      .curl-code .c-cmd { color: #a78bfa; }
      .curl-code .c-flag { color: #60a5fa; }
      .curl-code .c-url { color: #fbbf24; }
      .curl-code .c-str { color: #4ade80; }

      .links-grid {
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 0.75rem;
      }

      .link-card {
        display: flex;
        align-items: center;
        gap: 0.75rem;
        padding: 0.875rem 1rem;
        background: #18181b;
        border: 1px solid #27272a;
        border-radius: 8px;
        color: #d4d4d8;
        text-decoration: none;
        font-size: 0.85rem;
        transition: all 0.15s;
      }

      .link-card:hover {
        background: #1f1f23;
        border-color: #3f3f46;
        color: #fff;
      }

      .link-icon {
        flex-shrink: 0;
        width: 18px;
        height: 18px;
        color: #71717a;
      }

      .link-card:hover .link-icon { color: #a1a1aa; }

      footer {
        margin-top: 3rem;
        padding-top: 1.5rem;
        border-top: 1px solid #1c1c1f;
        font-size: 0.75rem;
        color: #52525b;
      }

      @media (max-width: 480px) {
        .page { padding: 2rem 1rem 3rem; }
        header h1 { font-size: 1.5rem; }
        .links-grid { grid-template-columns: 1fr; }
        .endpoint-desc { display: none; }
      }
    </style>
  </head>
  <body>
    <div class="page">
      <header>
        <h1>Mastra Server</h1>
        <p>Your server is running. Use the endpoints below to get started.</p>
        <div class="status-bar">
          <span class="status-dot"></span>
          <span id="base-url"></span>
        </div>
      </header>

      <section>
        <h2>Quick Start</h2>

        <div class="curl-block">
          <div class="curl-label">
            <span>Check server health</span>
            <button class="copy-btn" onclick="copyCode(this)">Copy</button>
          </div>
          <pre class="curl-code" data-tpl="curl <base>/health"><span class="c-cmd">curl</span> <span class="c-url" data-url="health"></span></pre>
        </div>

        <div class="curl-block">
          <div class="curl-label">
            <span>List your agents</span>
            <button class="copy-btn" onclick="copyCode(this)">Copy</button>
          </div>
          <pre class="curl-code" data-tpl="curl <base>${prefix}/agents"><span class="c-cmd">curl</span> <span class="c-url" data-url="${prefixNoSlash}/agents"></span></pre>
        </div>

        <div class="curl-block">
          <div class="curl-label">
            <span>Chat with an agent</span>
            <button class="copy-btn" onclick="copyCode(this)">Copy</button>
          </div>
          <pre class="curl-code" data-tpl="curl -X POST <base>${prefix}/agents/:agent-id/generate -H 'Content-Type: application/json' -d '{&quot;messages&quot;:[{&quot;role&quot;:&quot;user&quot;,&quot;content&quot;:&quot;Hello&quot;}]}'"><span class="c-cmd">curl</span> <span class="c-flag">-X POST</span> <span class="c-url" data-url="${prefixNoSlash}/agents/:agent-id/generate"></span> \\
  <span class="c-flag">-H</span> <span class="c-str">'Content-Type: application/json'</span> \\
  <span class="c-flag">-d</span> <span class="c-str">'{"messages":[{"role":"user","content":"Hello"}]}'</span></pre>
        </div>

        <div class="curl-block">
          <div class="curl-label">
            <span>Stream an agent response</span>
            <button class="copy-btn" onclick="copyCode(this)">Copy</button>
          </div>
          <pre class="curl-code" data-tpl="curl -N -X POST <base>${prefix}/agents/:agent-id/stream -H 'Content-Type: application/json' -d '{&quot;messages&quot;:[{&quot;role&quot;:&quot;user&quot;,&quot;content&quot;:&quot;Hello&quot;}]}'"><span class="c-cmd">curl</span> <span class="c-flag">-N -X POST</span> <span class="c-url" data-url="${prefixNoSlash}/agents/:agent-id/stream"></span> \\
  <span class="c-flag">-H</span> <span class="c-str">'Content-Type: application/json'</span> \\
  <span class="c-flag">-d</span> <span class="c-str">'{"messages":[{"role":"user","content":"Hello"}]}'</span></pre>
        </div>
      </section>

      <section>
        <h2>API Endpoints</h2>
        <div class="card">
          <div class="card-row">
            <div class="card-row-left">
              <span class="method method-get">GET</span>
              <span class="endpoint-path">${prefix}/agents</span>
            </div>
            <span class="endpoint-desc">List all agents</span>
          </div>
          <div class="card-row">
            <div class="card-row-left">
              <span class="method method-post">POST</span>
              <span class="endpoint-path">${prefix}/agents/:id/generate</span>
            </div>
            <span class="endpoint-desc">Generate a response</span>
          </div>
          <div class="card-row">
            <div class="card-row-left">
              <span class="method method-post">POST</span>
              <span class="endpoint-path">${prefix}/agents/:id/stream</span>
            </div>
            <span class="endpoint-desc">Stream a response</span>
          </div>
          <div class="card-row">
            <div class="card-row-left">
              <span class="method method-get">GET</span>
              <span class="endpoint-path">${prefix}/workflows</span>
            </div>
            <span class="endpoint-desc">List all workflows</span>
          </div>
          <div class="card-row">
            <div class="card-row-left">
              <span class="method method-post">POST</span>
              <span class="endpoint-path">${prefix}/workflows/:id/start</span>
            </div>
            <span class="endpoint-desc">Run a workflow</span>
          </div>
          <div class="card-row">
            <div class="card-row-left">
              <span class="method method-get">GET</span>
              <span class="endpoint-path">${prefix}/tools</span>
            </div>
            <span class="endpoint-desc">List all tools</span>
          </div>
          <div class="card-row">
            <div class="card-row-left">
              <span class="method method-post">POST</span>
              <span class="endpoint-path">${prefix}/tools/:id/execute</span>
            </div>
            <span class="endpoint-desc">Execute a tool</span>
          </div>
          <div class="card-row">
            <div class="card-row-left">
              <span class="method method-get">GET</span>
              <span class="endpoint-path">${prefix}/memory/threads</span>
            </div>
            <span class="endpoint-desc">List memory threads</span>
          </div>
        </div>
      </section>

      <section>
        <h2>Resources</h2>
        <div class="links-grid">
          <a href="https://mastra.ai/docs" target="_blank" rel="noopener noreferrer" class="link-card">
            <svg class="link-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4 19.5A2.5 2.5 0 0 1 6.5 17H20"/><path d="M6.5 2H20v20H6.5A2.5 2.5 0 0 1 4 19.5v-15A2.5 2.5 0 0 1 6.5 2z"/></svg>
            Documentation
          </a>
          <a href="https://mastra.ai/docs/server-db/custom-api-routes" target="_blank" rel="noopener noreferrer" class="link-card">
            <svg class="link-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M16 18l6-6-6-6"/><path d="M8 6l-6 6 6 6"/></svg>
            Custom API Routes
          </a>
          <a href="https://mastra.ai/docs/agents/overview" target="_blank" rel="noopener noreferrer" class="link-card">
            <svg class="link-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="3"/><path d="M12 1v4m0 14v4m-7.8-15.4 2.8 2.8m9.6 9.6 2.8 2.8M1 12h4m14 0h4M4.2 19.8l2.8-2.8m9.6-9.6 2.8-2.8"/></svg>
            Agents Guide
          </a>
          <a href="https://mastra.ai/docs/workflows/overview" target="_blank" rel="noopener noreferrer" class="link-card">
            <svg class="link-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="16 3 21 3 21 8"/><line x1="4" y1="20" x2="21" y2="3"/><polyline points="21 16 21 21 16 21"/><line x1="15" y1="15" x2="21" y2="21"/><line x1="4" y1="4" x2="9" y2="9"/></svg>
            Workflows Guide
          </a>
        </div>
      </section>

      <footer>
        Powered by <a href="https://mastra.ai" target="_blank" rel="noopener noreferrer" style="color:#71717a;">Mastra</a>
      </footer>
    </div>

    <script>
      (function() {
        var base = location.origin;
        document.getElementById('base-url').textContent = base;

        var els = document.querySelectorAll('[data-url]');
        for (var i = 0; i < els.length; i++) {
          els[i].textContent = base + '/' + els[i].getAttribute('data-url');
        }

        var tpls = document.querySelectorAll('[data-tpl]');
        for (var i = 0; i < tpls.length; i++) {
          tpls[i].setAttribute('data-tpl', tpls[i].getAttribute('data-tpl').replace(/<base>/g, base));
        }
      })();

      function copyCode(btn) {
        var pre = btn.closest('.curl-block').querySelector('.curl-code');
        var text = pre.getAttribute('data-tpl');
        if (!navigator.clipboard || !navigator.clipboard.writeText) {
          btn.textContent = 'Unavailable';
          setTimeout(function() { btn.textContent = 'Copy'; }, 1500);
          return;
        }
        navigator.clipboard.writeText(text).then(function() {
          btn.textContent = 'Copied!';
          setTimeout(function() { btn.textContent = 'Copy'; }, 1500);
        }).catch(function() {
          btn.textContent = 'Failed';
          setTimeout(function() { btn.textContent = 'Copy'; }, 1500);
        });
      }
    <\/script>
  </body>
</html>
`;
}
//#endregion
//#region src/server/index.ts
const getStudioPath = () => {
	if (process.env.MASTRA_STUDIO_PATH) return process.env.MASTRA_STUDIO_PATH;
	let __dirname = ".";
	if (import.meta.url) __dirname = dirname(fileURLToPath(import.meta.url));
	return process.env.MASTRA_STUDIO_PATH || join(__dirname, "studio");
};
function isSchemaApiRoute(route) {
	return "_mastraSchemaRoute" in route && route._mastraSchemaRoute === true;
}
const DEFAULT_CORS_ALLOW_METHODS = [
	"GET",
	"POST",
	"PUT",
	"PATCH",
	"DELETE",
	"OPTIONS"
];
const DEFAULT_CORS_ALLOW_HEADERS = [
	"Content-Type",
	"Authorization",
	"A2A-Version",
	"x-mastra-client-type",
	"x-mastra-dev-playground"
];
const DEFAULT_CORS_EXPOSE_HEADERS = ["Content-Length", "X-Requested-With"];
function getCorsConfig(serverCors, credentialsDefault) {
	const userCors = serverCors && typeof serverCors === "object" ? serverCors : void 0;
	const origin = userCors && "origin" in userCors && userCors.origin ? userCors.origin : credentialsDefault ? (requestOrigin) => requestOrigin || void 0 : "*";
	const credentials = userCors && "credentials" in userCors ? userCors.credentials : credentialsDefault;
	return {
		origin,
		allowMethods: DEFAULT_CORS_ALLOW_METHODS,
		credentials,
		maxAge: 3600,
		...userCors,
		allowHeaders: [...DEFAULT_CORS_ALLOW_HEADERS, ...userCors?.allowHeaders ?? []],
		exposeHeaders: [...DEFAULT_CORS_EXPOSE_HEADERS, ...userCors?.exposeHeaders ?? []]
	};
}
function getRouteCorsConfig(apiRoutes, pathname, method) {
	const route = findMatchingCustomRoute(pathname, method, apiRoutes)?.route;
	return route && !isSchemaApiRoute(route) ? route.cors : void 0;
}
function getToolExports(tools) {
	try {
		return tools.reduce((acc, toolModule) => {
			Object.entries(toolModule).forEach(([key, tool]) => {
				if (tool instanceof Tool) acc[key] = tool;
			});
			return acc;
		}, {});
	} catch (err) {
		console.error(`Failed to import tools
reason: ${err.message}
${err.stack.split("\n").slice(1).join("\n")}
    `, err);
	}
}
async function createHonoServer(mastra, options = { tools: {} }) {
	if (options.tools) for (const [key, tool] of Object.entries(options.tools)) try {
		mastra.addTool(tool, key);
	} catch {}
	const app = new Hono();
	const server = mastra.getServer();
	const apiPrefix = server?.apiPrefix ?? "/api";
	const a2aTaskStore = new InMemoryTaskStore();
	const processedRoutes = (server?.apiRoutes)?.map((route) => {
		if (isSchemaApiRoute(route)) return route;
		if (route.openapi) {
			const existingMiddleware = route.middleware ? Array.isArray(route.middleware) ? route.middleware : [route.middleware] : [];
			return {
				...route,
				middleware: [describeRoute(route.openapi), ...existingMiddleware]
			};
		}
		return route;
	});
	const customRouteAuthConfig = /* @__PURE__ */ new Map();
	if (processedRoutes) for (const route of processedRoutes) {
		const requiresAuth = route.requiresAuth !== false;
		const routeKey = `${route.method}:${route.path}`;
		customRouteAuthConfig.set(routeKey, requiresAuth);
	}
	const customOnError = server?.onError;
	app.onError((err, c) => {
		if (customOnError) return customOnError(err, c);
		return errorHandler(err, c, options.isDev);
	});
	const bodyLimitOptions = {
		maxSize: server?.bodySizeLimit ?? 4.5 * 1024 * 1024,
		onError: () => ({ error: "Request body too large" })
	};
	const honoServerAdapter = new MastraServer$1({
		app,
		mastra,
		tools: options.tools,
		taskStore: a2aTaskStore,
		bodyLimitOptions,
		openapiPath: options?.isDev || server?.build?.openAPIDocs ? "/openapi.json" : void 0,
		customRouteAuthConfig,
		customApiRoutes: processedRoutes,
		prefix: apiPrefix,
		mcpOptions: server?.mcpOptions
	});
	honoServerAdapter.registerContextMiddleware();
	const serverMiddleware = mastra.getServerMiddleware?.();
	if (serverMiddleware && serverMiddleware.length > 0) for (const m of serverMiddleware) app.use(m.path, skipIfFrameworkPublic(m.handler));
	const browserStreamSetup = options.browserStream === false ? null : await setupBrowserStream(app, {
		getToolset: async (agentId) => {
			const resolveBrowser = async (agent) => {
				if (!agent) return void 0;
				if (agent.browser) return agent.browser;
				try {
					return (await agent.getWorkspace())?.browser;
				} catch {
					return;
				}
			};
			try {
				const runtimeAgent = mastra.getAgentById(agentId);
				if (runtimeAgent) return await resolveBrowser(runtimeAgent);
			} catch {}
			try {
				return await resolveBrowser(await mastra.getEditor?.()?.agent.getById(agentId));
			} catch {
				return;
			}
		},
		apiPrefix
	});
	if (!browserStreamSetup) app.get(`${apiPrefix}/agents/:agentId/browser/session`, (c) => c.json({
		hasSession: false,
		screencastAvailable: false
	}));
	if (server?.cors === false) app.use("*", timeout(server?.timeout ?? 180 * 1e3));
	else {
		const hasAuth = !!server?.auth;
		app.use("*", timeout(server?.timeout ?? 180 * 1e3), async (c, next) => {
			const pathname = new URL(c.req.url).pathname;
			const method = c.req.method === "OPTIONS" ? c.req.header("Access-Control-Request-Method") ?? c.req.method : c.req.method;
			const routeCors = getRouteCorsConfig(processedRoutes, pathname, method);
			return cors(routeCors ? getCorsConfig(routeCors, false) : getCorsConfig(server?.cors, hasAuth))(c, next);
		});
	}
	app.get("/health", describeRoute({
		description: "Health check endpoint",
		tags: ["system"],
		responses: { 200: { description: "Service is healthy" } }
	}), healthHandler);
	if (options?.isDev && options.studio) app.get("/api/learning/*", agentLearningProxyHandler);
	if (options?.isDev || server?.build?.swaggerUI) app.get(apiPrefix, describeRoute({
		description: "API Welcome Page",
		tags: ["system"],
		responses: { 200: { description: "Success" } }
	}), rootHandler);
	await honoServerAdapter.validateEELicense();
	honoServerAdapter.registerAuthMiddleware();
	if (server?.middleware) {
		const middlewares = (Array.isArray(server.middleware) ? server.middleware : [server.middleware]).map((middleware) => {
			if (typeof middleware === "function") return {
				path: "*",
				handler: middleware
			};
			return middleware;
		});
		for (const middleware of middlewares) app.use(middleware.path, skipIfFrameworkPublic(middleware.handler));
	}
	await honoServerAdapter.registerCustomApiRoutes();
	if (server?.build?.apiReqLogs) app.use(logger());
	await honoServerAdapter.registerRoutes();
	if (options?.isDev || server?.build?.swaggerUI) {
		if (!options?.isDev && server?.build?.swaggerUI && !server?.build?.openAPIDocs) mastra.getLogger().warn("Swagger UI is enabled but OpenAPI documentation is disabled. The Swagger UI will not function properly without the OpenAPI endpoint. Please enable openAPIDocs in your server.build configuration:\n  server: { build: { swaggerUI: true, openAPIDocs: true } }");
		app.get("/swagger-ui", describeRoute({ hide: true }), middleware({ url: `${apiPrefix}/openapi.json` }));
	}
	if (options?.isDev) app.post("/__restart-active-workflow-runs", describeRoute({ hide: true }), restartAllActiveWorkflowRunsHandler);
	const serverOptions = mastra.getServer();
	const studioBasePath = normalizeStudioBase(serverOptions?.studioBase ?? "/");
	const devServerInstanceId = options?.isDev ? globalThis.crypto.randomUUID() : void 0;
	if (options?.studio) {
		const studioControlRoutes = [
			{
				method: "GET",
				path: "/refresh-events",
				responseType: "datastream-response",
				handler: async ({ abortSignal }) => handleClientsRefreshRequest(abortSignal, devServerInstanceId)
			},
			{
				method: "POST",
				path: "/__refresh",
				responseType: "json",
				handler: async () => getTriggerClientsRefreshPayload()
			},
			{
				method: "GET",
				path: "/__hot-reload-status",
				responseType: "json",
				handler: async () => ({
					disabled: isHotReloadDisabled(),
					timestamp: (/* @__PURE__ */ new Date()).toISOString()
				})
			}
		];
		for (const route of studioControlRoutes) {
			customRouteAuthConfig.set(`${route.method}:${studioBasePath}${route.path}`, !options.isDev);
			await honoServerAdapter.registerRoute(app, route, { prefix: studioBasePath });
		}
		app.use(`${studioBasePath}/assets/*`, compress());
		const studioPath = getStudioPath();
		app.use(`${studioBasePath}/assets/*`, serveStatic({
			root: join(studioPath, "assets"),
			rewriteRequestPath: (path) => {
				let rewritten = path;
				if (studioBasePath && rewritten.startsWith(studioBasePath)) rewritten = rewritten.slice(studioBasePath.length);
				if (rewritten.startsWith("/assets")) rewritten = rewritten.slice(7);
				return rewritten;
			}
		}));
	}
	app.get("*", async (c, next) => {
		const requestPath = c.req.path;
		if (requestPath === apiPrefix || requestPath.startsWith(`${apiPrefix}/`) || requestPath.startsWith("/swagger-ui") || requestPath.startsWith("/openapi.json")) return await next();
		if (requestPath.includes(".") && !requestPath.endsWith(".html")) return await next();
		const isStudioRoute = studioBasePath === "" || requestPath === studioBasePath || requestPath.startsWith(`${studioBasePath}/`);
		if (options?.studio && isStudioRoute) {
			let indexHtml = await readFile(join(getStudioPath(), "index.html"), "utf-8");
			const port = serverOptions?.port ?? (Number(process.env.PORT) || 4111);
			const hideCloudCta = process.env.MASTRA_HIDE_CLOUD_CTA === "true";
			const host = serverOptions?.host ?? process.env.MASTRA_HOST ?? "localhost";
			const key = serverOptions?.https?.key ?? (process.env.MASTRA_HTTPS_KEY ? Buffer.from(process.env.MASTRA_HTTPS_KEY, "base64") : void 0);
			const cert = serverOptions?.https?.cert ?? (process.env.MASTRA_HTTPS_CERT ? Buffer.from(process.env.MASTRA_HTTPS_CERT, "base64") : void 0);
			const protocol = key && cert ? "https" : "http";
			const studioHost = serverOptions?.studioHost ?? host;
			const studioProtocol = serverOptions?.studioProtocol ?? protocol;
			const studioPort = serverOptions?.studioPort ?? port;
			const cloudApiEndpoint = process.env.MASTRA_CLOUD_API_ENDPOINT || "";
			const experimentalFeatures = process.env.EXPERIMENTAL_FEATURES === "true" ? "true" : "false";
			const experimentalUI = process.env.MASTRA_EXPERIMENTAL_UI === "true" ? "true" : "false";
			const templatesEnabled = process.env.MASTRA_TEMPLATES === "true" ? "true" : "false";
			const agentSignals = process.env.MASTRA_AGENT_SIGNALS === "false" ? "false" : "true";
			const signalsUI = process.env.MASTRA_SIGNALS_UI === "true" ? "true" : "false";
			const organizationId = process.env.MASTRA_ORGANIZATION_ID || "";
			const platformProjectId = process.env.MASTRA_PLATFORM_PROJECT_ID || "";
			const platformObservabilityEndpoint = process.env.MASTRA_PLATFORM_OBSERVABILITY_ENDPOINT || "";
			const requestContextPresets = process.env.MASTRA_REQUEST_CONTEXT_PRESETS || "";
			const autoDetectUrl = process.env.MASTRA_AUTO_DETECT_URL === "true";
			indexHtml = injectStudioHtmlConfig(indexHtml, {
				host: `'${studioHost}'`,
				port: `'${studioPort}'`,
				protocol: `'${studioProtocol}'`,
				apiPrefix: `'${serverOptions?.apiPrefix ?? "/api"}'`,
				basePath: studioBasePath,
				hideCloudCta: `'${hideCloudCta}'`,
				cloudApiEndpoint: `'${escapeStudioHtmlValue(cloudApiEndpoint)}'`,
				experimentalFeatures: `'${experimentalFeatures}'`,
				templates: `'${templatesEnabled}'`,
				telemetryDisabled: `'${escapeStudioHtmlValue(process.env.MASTRA_TELEMETRY_DISABLED ?? "")}'`,
				requestContextPresets: `'${escapeStudioHtmlValue(requestContextPresets)}'`,
				experimentalUI: `'${experimentalUI}'`,
				agentSignals: `'${agentSignals}'`,
				signalsUI: `'${signalsUI}'`,
				organizationId: `'${escapeStudioHtmlValue(organizationId)}'`,
				platformProjectId: `'${escapeStudioHtmlValue(platformProjectId)}'`,
				platformObservabilityEndpoint: `'${escapeStudioHtmlValue(platformObservabilityEndpoint)}'`,
				autoDetectUrl: `'${autoDetectUrl}'`,
				devServerInstanceId: JSON.stringify(devServerInstanceId ?? "")
			});
			return c.newResponse(indexHtml, 200, { "Content-Type": "text/html" });
		}
		return c.newResponse(welcomeHtml(apiPrefix), 200, { "Content-Type": "text/html" });
	});
	if (options?.studio) {
		const studioRootPath = getStudioPath();
		const studioPath = studioBasePath ? `${studioBasePath}/*` : "*";
		app.use(studioPath, serveStatic({
			root: studioRootPath,
			rewriteRequestPath: (path) => {
				if (studioBasePath && path.startsWith(studioBasePath)) return path.slice(studioBasePath.length);
				return path;
			}
		}));
	}
	app.injectWebSocket = browserStreamSetup?.injectWebSocket;
	return app;
}
async function createNodeServer(mastra, options = { tools: {} }) {
	const app = await createHonoServer(mastra, options);
	const injectWebSocket = app.injectWebSocket;
	const serverOptions = mastra.getServer();
	const apiPrefix = serverOptions?.apiPrefix ?? "/api";
	const drainTimeoutMs = serverOptions?.drainTimeout ?? 5e3;
	if (serverOptions?.handleShutdownSignals !== false && (!Number.isFinite(drainTimeoutMs) || drainTimeoutMs < 0 || drainTimeoutMs > 2147483647)) throw new RangeError("server.drainTimeout must be a finite number between 0 and 2147483647 milliseconds");
	const key = serverOptions?.https?.key ?? (process.env.MASTRA_HTTPS_KEY ? Buffer.from(process.env.MASTRA_HTTPS_KEY, "base64") : void 0);
	const cert = serverOptions?.https?.cert ?? (process.env.MASTRA_HTTPS_CERT ? Buffer.from(process.env.MASTRA_HTTPS_CERT, "base64") : void 0);
	const isHttpsEnabled = Boolean(key && cert);
	const bindHost = serverOptions?.host ?? process.env.MASTRA_HOST;
	const host = bindHost ?? "localhost";
	const port = serverOptions?.port ?? (Number(process.env.PORT) || 4111);
	const protocol = isHttpsEnabled ? "https" : "http";
	const studioHost = serverOptions?.studioHost ?? host;
	const studioProtocol = serverOptions?.studioProtocol ?? protocol;
	const studioPort = serverOptions?.studioPort ?? port;
	const server = serve({
		fetch: app.fetch,
		port,
		hostname: bindHost,
		...isHttpsEnabled ? {
			createServer: https.createServer,
			serverOptions: {
				key,
				cert
			}
		} : {}
	}, () => {
		const logger = mastra.getLogger();
		logger.info("Mastra API running", { url: `${protocol}://${host}:${port}${apiPrefix}` });
		if (options?.studio) {
			const studioBasePath = normalizeStudioBase(serverOptions?.studioBase ?? "/");
			const studioUrl = `${studioProtocol}://${studioHost}:${studioPort}${studioBasePath}`;
			logger.info("Studio available", { url: studioUrl });
		}
		if (process.send) process.send({
			type: "server-ready",
			port,
			host
		});
	});
	injectWebSocket?.(server);
	const workerLifecycle = mastra;
	if (typeof workerLifecycle.startWorkers === "function") await workerLifecycle.startWorkers();
	else await workerLifecycle.startEventEngine();
	import('@mastra/core/telemetry').then(({ syncUsageTelemetry }) => syncUsageTelemetry(mastra)).catch(() => {});
	import('@mastra/core/telemetry').then(({ syncFeatureUsageTelemetry }) => syncFeatureUsageTelemetry(mastra)).catch(() => {});
	if (serverOptions?.handleShutdownSignals !== false) {
		const CORE_SHUTDOWN_TIMEOUT_MS = 5e3;
		const timedOut = Symbol("shutdown-timeout");
		const race = async (work, ms) => {
			let timeout;
			const timeoutPromise = new Promise((resolve) => {
				timeout = setTimeout(() => resolve(timedOut), ms);
			});
			try {
				return await Promise.race([work, timeoutPromise]);
			} finally {
				clearTimeout(timeout);
			}
		};
		let shuttingDown = false;
		const shutdown = async (signal) => {
			if (shuttingDown) {
				process.exit(1);
				return;
			}
			shuttingDown = true;
			const logger = mastra.getLogger();
			logger.info("Shutting down Mastra server", { signal });
			try {
				const drained = new Promise((resolve) => {
					server.close(() => resolve());
				});
				closeRefreshStreams();
				const closeIdleConnections = () => server.closeIdleConnections?.();
				closeIdleConnections();
				const idleSweep = setInterval(closeIdleConnections, 100);
				idleSweep.unref?.();
				try {
					if (await race(drained, drainTimeoutMs) === timedOut) {
						logger.warn("Mastra server drain timed out; closing remaining HTTP connections", { timeoutMs: drainTimeoutMs });
						server.closeAllConnections?.();
					}
				} finally {
					clearInterval(idleSweep);
				}
			} catch (error) {
				logger.error("Error while draining Mastra server", { error });
			}
			try {
				const lifecycle = mastra;
				if (typeof lifecycle.shutdown === "function") {
					const coreTimeoutMs = drainTimeoutMs + CORE_SHUTDOWN_TIMEOUT_MS;
					if (await race(lifecycle.shutdown({ drainTimeout: drainTimeoutMs }), coreTimeoutMs) === timedOut) logger.warn("Mastra shutdown timed out; forcing exit", { timeoutMs: coreTimeoutMs });
				}
			} catch (error) {
				logger.error("Error during Mastra shutdown", { error });
			}
			process.exit(0);
		};
		process.on("SIGINT", () => void shutdown("SIGINT"));
		process.on("SIGTERM", () => void shutdown("SIGTERM"));
	}
	return server;
}

// @ts-ignore
// @ts-ignore
await createNodeServer(mastra, {
  studio: true,
  isDev: true,
  tools: getToolExports(tools),
});

if (mastra.getStorage()) {
  mastra.__registerInternalWorkflow(scoreTracesWorkflow);
}
