/**
 * Model reference helpers shared by the LLM runners and the memory pipeline.
 *
 * A model reference may be written either as a bare model name
 * ("qwen3.5:9b-mlx") or in "provider/model" form ("openai/qwen3.5:9b-mlx").
 * The OpenAI-compatible wire protocol only carries the model name, so the
 * provider prefix is stripped before the name reaches the API.
 */

/**
 * Normalize a model reference down to the bare model name.
 *
 * - "provider/model" → "model" (everything after the first "/")
 * - "model"          → "model" (unchanged)
 * - "" / undefined   → undefined (caller keeps its default)
 */
export function normalizeModelName(ref: string | undefined): string | undefined {
  if (!ref) return undefined;
  const trimmed = ref.trim();
  if (!trimmed) return undefined;

  const slashIdx = trimmed.indexOf("/");
  // slashIdx === -1 → bare name; slashIdx === 0 → leading "/" is malformed.
  if (slashIdx <= 0) return trimmed;

  const model = trimmed.slice(slashIdx + 1).trim();
  return model || undefined;
}
