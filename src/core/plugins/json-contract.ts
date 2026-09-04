import { isAbsolute } from 'node:path';

export type JsonSchema = Record<string, unknown>;

const SECRET_KEY = /(?:^|[_-])(token|secret|password|credential|authorization|cookie|api[_-]?key)(?:$|[_-])/i;
const PATH_KEY = /(?:^|[_-]|[a-z])(path|file|dir|directory|cwd)(?:$|[_-])/i;

function isRecord(value: unknown): value is Record<string, unknown> {
  return !!value && typeof value === 'object' && !Array.isArray(value);
}

/** Small, deterministic JSON-schema subset used at the plugin trust boundary.
 * Unsupported keywords fail closed instead of silently weakening validation. */
export function assertJsonSchemaValue(schema: JsonSchema, value: unknown, at = '$'): void {
  const allowed = new Set(['type', 'properties', 'required', 'additionalProperties', 'items', 'enum', 'const', 'maxLength', 'maximum', 'minimum']);
  for (const key of Object.keys(schema)) {
    if (!allowed.has(key)) throw new Error(`plugin_schema_keyword_unsupported:${at}:${key}`);
  }
  if (Array.isArray(schema.enum) && !schema.enum.some(item => JSON.stringify(item) === JSON.stringify(value))) {
    throw new Error(`plugin_schema_invalid:${at}:enum`);
  }
  if (Object.hasOwn(schema, 'const') && JSON.stringify(schema.const) !== JSON.stringify(value)) {
    throw new Error(`plugin_schema_invalid:${at}:const`);
  }
  const type = schema.type;
  const validType = type === 'object' ? isRecord(value)
    : type === 'array' ? Array.isArray(value)
    : type === 'string' ? typeof value === 'string'
    : type === 'number' ? typeof value === 'number' && Number.isFinite(value)
    : type === 'integer' ? Number.isSafeInteger(value)
    : type === 'boolean' ? typeof value === 'boolean'
    : type === 'null' ? value === null
    : false;
  if (!validType) throw new Error(`plugin_schema_invalid:${at}:type`);
  if (typeof value === 'string' && typeof schema.maxLength === 'number' && value.length > schema.maxLength) {
    throw new Error(`plugin_schema_invalid:${at}:maxLength`);
  }
  if (typeof value === 'number') {
    if (typeof schema.maximum === 'number' && value > schema.maximum) throw new Error(`plugin_schema_invalid:${at}:maximum`);
    if (typeof schema.minimum === 'number' && value < schema.minimum) throw new Error(`plugin_schema_invalid:${at}:minimum`);
  }
  if (Array.isArray(value)) {
    if (!isRecord(schema.items)) throw new Error(`plugin_schema_invalid:${at}:items`);
    value.forEach((item, i) => assertJsonSchemaValue(schema.items as JsonSchema, item, `${at}[${i}]`));
  }
  if (isRecord(value)) {
    const properties = schema.properties === undefined ? {} : schema.properties;
    if (!isRecord(properties)) throw new Error(`plugin_schema_invalid:${at}:properties`);
    const required = schema.required === undefined ? [] : schema.required;
    if (!Array.isArray(required) || required.some(key => typeof key !== 'string')) throw new Error(`plugin_schema_invalid:${at}:required`);
    for (const key of required as string[]) if (!Object.hasOwn(value, key)) throw new Error(`plugin_schema_invalid:${at}.${key}:required`);
    for (const [key, child] of Object.entries(value)) {
      const childSchema = properties[key];
      if (childSchema === undefined) {
        if (schema.additionalProperties !== true) throw new Error(`plugin_schema_invalid:${at}.${key}:additionalProperty`);
      } else if (!isRecord(childSchema)) {
        throw new Error(`plugin_schema_invalid:${at}.${key}:schema`);
      } else {
        assertJsonSchemaValue(childSchema, child, `${at}.${key}`);
      }
    }
  }
}

/** Public plugin DTOs may not carry credentials or host filesystem paths. */
export function assertSafePluginDto(value: unknown, at = '$'): void {
  JSON.stringify(value); // reject cyclic values
  const visit = (current: unknown, location: string): void => {
    if (typeof current === 'bigint' || typeof current === 'function' || typeof current === 'symbol' || current === undefined) {
      throw new Error(`plugin_dto_not_json:${location}`);
    }
    if (typeof current === 'number' && !Number.isFinite(current)) throw new Error(`plugin_dto_not_json:${location}`);
    if (Array.isArray(current)) return current.forEach((item, i) => visit(item, `${location}[${i}]`));
    if (!isRecord(current)) return;
    for (const [key, child] of Object.entries(current)) {
      if (SECRET_KEY.test(key)) throw new Error(`plugin_dto_secret_forbidden:${location}.${key}`);
      if (PATH_KEY.test(key) && typeof child === 'string' && isAbsolute(child)) {
        throw new Error(`plugin_dto_absolute_path_forbidden:${location}.${key}`);
      }
      visit(child, `${location}.${key}`);
    }
  };
  visit(value, at);
}
