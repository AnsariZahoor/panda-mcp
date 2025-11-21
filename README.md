# Panda MCP Server

A Model Context Protocol (MCP) server for cryptocurrency exchange data and analytics.

## Features

- **Exchange Data**: Real-time OHLCV, funding rates, and open interest from multiple exchanges
- **Technical Indicators**: 13+ standard TA indicators (RSI, MACD, Bollinger Bands, etc.)
- **JLabs Models**: Proprietary analytics including Divine Dip, Slippage, Price Equilibrium, CARI, DXY Risk, and ROSI
- **Orderbook Metrics**: Depth analysis with bid/ask ratios and volume deltas
- **Orderflow Metrics**: Trade flow analysis with volume-based filtering
- **Enterprise Security**: API key authentication, rate limiting, input validation, and audit logging

## Supported Exchanges

- Binance (Spot & Futures)
- Bybit (Spot & Futures)
- Hyperliquid (Futures)
- OKX
- Kraken

## Installation

### Local Development

```bash
# Clone repository
git clone https://github.com/yourusername/panda-mcp.git
cd panda-mcp

# Install dependencies
pip install -e .

# Configure environment
cp .env.example .env
# Edit .env with your API keys

# Run server
python -m panda_mcp.server
```

### Docker Deployment

```bash
# Build and run with Docker Compose
docker compose up -d

# View logs
docker compose logs -f

# Stop server
docker compose down
```

## Configuration

Create a `.env` file with your configuration:

```bash
# Panda Backend API
PANDA_BACKEND_API_URL=https://your-api-domain.com
PANDA_API_KEY=your-api-key

# Security
PANDA_AUTH_ENABLED=true
PANDA_RATE_LIMIT_ENABLED=true
PANDA_VALIDATION_ENABLED=true
PANDA_AUDIT_ENABLED=true

# API Keys for Users
PANDA_API_KEY_USER1=sk_live_your_secure_key_here
PANDA_API_KEY_USER1_USER=user1
```

## Usage with Claude Desktop

Add to your Claude Desktop config (`~/Library/Application Support/Claude/claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "panda-mcp": {
      "command": "python",
      "args": ["-m", "panda_mcp.server"],
      "env": {
        "PANDA_BACKEND_API_URL": "https://your-api-domain.com",
        "PANDA_API_KEY": "your-api-key"
      }
    }
  }
}
```

## Available Metrics

### Exchange Data
- OHLCV candlestick data
- Funding rates
- Open interest
- Trading pairs
- Market data

### Technical Indicators
- SMA, EMA, RSI, MACD
- Bollinger Bands, ATR
- Stochastic, ADX, CCI
- OBV, VWAP, Williams %R

### JLabs Models
- **Divine Dip**: Buy opportunity detection
- **Slippage**: Price impact analysis
- **Price Equilibrium**: Stability metrics
- **CARI**: Asset risk indicator
- **DXY Risk**: Dollar correlation
- **ROSI**: Momentum metrics

### Orderbook Analysis
- Bid/ask ratios and deltas
- Volume CVD
- Depth analysis at multiple price levels

### Orderflow Analysis
- Trade volume by size
- Buy/sell pressure
- Cumulative delta
- Smart money tracking

## Security

The server includes enterprise-grade security features:

- **Authentication**: API key validation
- **Rate Limiting**: Configurable request limits (default: 100/min)
- **Input Validation**: SQL injection and XSS prevention
- **Audit Logging**: Complete request tracking

## Development

```bash
# Install development dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Format code
black src/

# Type checking
mypy src/
```

## Documentation

See the [docs](./docs) directory for:
- [EC2 Deployment Guide](./docs/EC2_DEPLOYMENT.md)
- [API Documentation](./docs/README.md)
- Security configuration examples

## License

[Your License Here]

## Support

For issues and questions, please open an issue on GitHub.
