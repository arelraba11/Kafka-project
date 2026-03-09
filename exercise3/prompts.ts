// ---------------------------------------------------------------------------
// REVIEW_ROUTER_PROMPT
// Technique: Zero-shot classification
// Purpose:   Determine if a message is a product review or should be ignored.
// ---------------------------------------------------------------------------
export function reviewRouterPrompt(userInput: string): string {
  return `You are a message classifier for a review processing pipeline.

Classify the following user message into one of these intents:
- analyzeReview   : The message is a product or service review containing an opinion.
- ignore          : The message is not a review (spam, greeting, test input, question, etc.).

Respond with valid JSON only. No explanation. No markdown.

Schema:
{
  "intent": "analyzeReview" | "ignore",
  "reason": string
}

Message:
"""
${userInput}
"""`;
}

// ---------------------------------------------------------------------------
// REVIEW_ANALYZER_PROMPT
// Technique: Structured JSON output
// Purpose:   Extract structured sentiment insights from review text.
// ---------------------------------------------------------------------------
export function reviewAnalyzerPrompt(reviewText: string): string {
  return `You are a sentiment analysis engine for product reviews.

Analyze the following review and return structured insights.

Rules:
- Respond with valid JSON only. No explanation. No markdown fences.
- score must be an integer between 1 and 10.
- overall_sentiment must be exactly one of: "Positive", "Negative", "Mixed", "Neutral".
- aspects is an array of { aspect: string, sentiment: "Positive" | "Negative" | "Neutral" }.
- summary must be one sentence, maximum 20 words.

Schema:
{
  "summary": string,
  "overall_sentiment": "Positive" | "Negative" | "Mixed" | "Neutral",
  "score": number,
  "aspects": [
    { "aspect": string, "sentiment": "Positive" | "Negative" | "Neutral" }
  ]
}

Review:
"""
${reviewText}
"""`;
}

// ---------------------------------------------------------------------------
// SELF_CORRECTION_PROMPT
// Technique: Self-correction / reflective prompting
// Purpose:   Resolve the contradiction when score < 4 but sentiment == "Positive".
// ---------------------------------------------------------------------------
export function selfCorrectionPrompt(
  reviewText: string,
  previousResult: string,
  score: number
): string {
  return `You previously analyzed a product review and returned this result:

${previousResult}

There is a logical inconsistency in this result:
- The score is ${score}/10, which indicates a poor or very poor review.
- Yet the overall_sentiment is "Positive", which contradicts a low score.

Please re-analyze the original review and return a corrected, consistent result.

Rules:
- Respond with valid JSON only. No explanation. No markdown fences.
- The corrected score and sentiment must be logically consistent.
- Use the same schema as before.

Original review:
"""
${reviewText}
"""`;
}
