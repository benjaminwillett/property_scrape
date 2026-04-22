# Assumptions

- Deposit: 20% of purchase price (override per user profile)
- Interest buffer: +2.0% on scenario rate
- Rent shading: 70% base, scenario-specific overrides
- Vacancy allowance: 2 weeks/year base; scenario adds more
- Maintenance: 7% of gross rent (adjusted by scenario)
- Property management: 6% of gross rent
- Insurance: $1,200 per annum default
- Council + water: $2,600 per annum default
- Entry costs: stamp duty (state rules, simplified bands), conveyancing $1,800, B&P $650, buyer’s agent 0% default, initial repairs 1%
- Exit costs: agent 1.8%, marketing $2,200, legals $1,800, other 1%
- CGT: 50% discount after 12 months; applies on estimated gain (approximate only)
- Scenarios:
  - Defensive: higher rates (7.5% + buffer), flat growth, 4 vacancy weeks, maintenance 8%, rent shading 65%
  - Balanced: base (6.5% + buffer), 2% growth, 2 vacancy weeks, maintenance 7%, rent shading 70%
  - Aggressive: lower rates (6% + buffer), 3.5% growth, 1 vacancy week, maintenance 6.5%, rent shading 75%
- Stress toggles: rate 8.5% or +200bps, rent -10%, vacancy +2 weeks, price -10% for 2 years
- Borrowing impact: servicing ratio 35% of income with lender buffer; rent shading applied before serviceability
- Profiles: free tier stores 1 assumptions profile, paid tier unlimited (persistence TODO)

> All figures are indicative only and not financial advice. Users must confirm with licensed professionals.
