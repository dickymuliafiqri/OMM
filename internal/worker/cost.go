package worker

// EstimateCost calculates approximate inference cost in USD and assigns a descriptive tier.
func EstimateCost(promptTokens, completionTokens int) (costUSD float64, tier string) {
	// Average market pricing fallback ($3/1M prompt tokens, $15/1M completion tokens)
	cost := (float64(promptTokens)/1_000_000.0 * 3.0) + (float64(completionTokens)/1_000_000.0 * 15.0)

	switch {
	case cost < 0.001:
		return cost, "Sangat Ekonomis"
	case cost < 0.005:
		return cost, "Ekonomis"
	case cost < 0.02:
		return cost, "Standar"
	default:
		return cost, "Premium"
	}
}
