// Ollama prompt templates used by sanitizer.ts

// Instructs the model to scrub PII from a customer message.
// Returns only the sanitized text — no explanation, no extra tokens.
export function buildSanitizePrompt(rawText: string): string {
  // TODO: implement prompt template
  // Requirements:
  //   - Replace all proper names with [NAME]
  //   - Replace all phone numbers (any format) with [NUMBER]
  //   - Preserve the original sentence structure and meaning
  //   - Return only the sanitized text, nothing else
  throw new Error("buildSanitizePrompt: not implemented");
}
