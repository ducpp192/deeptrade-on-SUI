This project is a funding rate arbitrage bot designed to capture funding rate differences between perpetual futures markets on different exchanges.

The bot continuously monitors funding rates for the same trading pairs across DeepTrade (Sui ecosystem) and Binance Futures. When a significant funding rate gap appears between the two exchanges, the bot opens delta-neutral positions to collect funding payments while minimizing market exposure.

The core strategy works as follows:

Funding Rate Monitoring
The bot regularly fetches funding rate data for supported trading pairs from both exchanges. It compares the current and upcoming funding rates to detect profitable discrepancies.

Opportunity Detection
When the difference between funding rates exceeds a predefined threshold, the bot identifies an arbitrage opportunity. For example, if one exchange has a high positive funding rate and the other has a lower or negative rate, traders can capture the spread.

Delta-Neutral Positioning
To remain market neutral, the bot opens two opposite positions simultaneously:

LONG on the exchange with the lower (or negative) funding rate

SHORT on the exchange with the higher funding rate

This hedged structure ensures that price movements have minimal impact on the overall position while funding payments accumulate.

Funding Capture
As funding payments are distributed periodically by perpetual futures markets, the bot collects the funding difference between the two exchanges.

Position Management
The bot continuously monitors the funding spread and market conditions. When the spread narrows or the trade is no longer profitable, it closes both positions to lock in the arbitrage profit.

Key Features

Cross-exchange funding rate monitoring

Automated arbitrage opportunity detection

Delta-neutral hedging strategy

Risk management through threshold controls

Designed for automated execution

Use Case

This tool is intended for traders and developers interested in systematic crypto arbitrage strategies, particularly funding rate capture across multiple derivatives exchanges.

In summary, the bot acts as an automated system that identifies funding rate inefficiencies between exchanges and executes hedged trades to generate yield from funding payments rather than directional price speculation.
