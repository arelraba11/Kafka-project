import type { FullInsight, PartialInsight, SentimentResult, UrgencyResult } from "./types.ts";

// Computes derived latency fields from a completed PartialInsight.
// Returns null if either branch is still missing.
export function computeBenchmark(partial: PartialInsight): FullInsight["benchmark"] | null {
  if (!partial.sentiment || !partial.urgency) return null;

  // TODO: implement latency calculations
  // sanitize_latency_ms  = t_sanitizer_out - t_produced
  // sentiment_latency_ms = t_sentiment_out - t_sanitizer_out
  // urgency_latency_ms   = t_urgency_out   - t_sanitizer_out
  // total_latency_ms     = max(t_sentiment_out, t_urgency_out) - t_produced

  return null;
}

// Merges a completed PartialInsight into a FullInsight.
// Caller must ensure both branches are non-null before calling.
export function mergeInsight(partial: PartialInsight): FullInsight {
  // TODO: implement merge
  throw new Error("mergeInsight: not implemented");
}
