# Hive to Snowflake SQL Converter - Installation Guide

## System Requirements

### Prerequisites
- Node.js 18.x or higher
- Python 3.8 or higher
- npm 9.x or higher
- Git (optional, for version control)

### Supported Operating Systems
- Linux
- macOS
- Windows (with WSL recommended for best performance)

## Installation Methods

### 1. Quick Start (Recommended)

```bash
# Clone the repository (if using Git)
git clone https://github.com/yourusername/hive-to-snowflake-converter
cd hive-to-snowflake-converter

# Install dependencies
npm install

# Start the development server
npm run dev
```

### 2. Manual Installation

1. Download the project files
2. Navigate to the project directory
3. Install dependencies:
   ```bash
   npm install
   ```
4. Install Python dependencies:
   ```bash
   pip install sqlglot
   ```

## Configuration

### Environment Variables
Create a `.env` file in the root directory:

```env
# Server Configuration
PORT=3000
HOST=localhost

# Python Path (if needed)
PYTHON_PATH=/usr/bin/python3
```

### Development Mode
```bash
# Start development server
npm run dev

# Run tests
npm test

# Run linter
npm run lint
```

### Production Mode
```bash
# Build the project
npm run build

# Start production server
npm run start
```

## Project Structure

```
hive-to-snowflake-converter/
├── src/
│   ├── converter/         # SQL conversion logic
│   ├── lib/              # Utility functions
│   ├── types/            # TypeScript type definitions
│   └── App.tsx           # Main React component
├── docs/                 # Documentation
├── public/              # Static assets
└── package.json         # Project configuration
```

## Verification

### Testing Installation
1. Start the development server:
   ```bash
   npm run dev
   ```
2. Open your browser to `http://localhost:3000`
3. Try converting a simple Hive SQL query:
   ```sql
   SELECT * FROM my_table;
   ```

### Common Issues

1. Python Not Found
   ```bash
   # Check Python installation
   python3 --version
   
   # Update PATH if needed
   export PATH=$PATH:/usr/local/bin
   ```

2. Node Modules Issues
   ```bash
   # Clear npm cache
   npm cache clean --force
   
   # Reinstall dependencies
   rm -rf node_modules
   npm install
   ```

3. Port Already in Use
   ```bash
   # Find process using port 3000
   lsof -i :3000
   
   # Kill process
   kill -9 <PID>
   ```

## Updating

### Updating Dependencies
```bash
# Update npm packages
npm update

# Update Python dependencies
pip install --upgrade sqlglot
```

### Version Control
```bash
# Get latest changes
git pull origin main

# Install new dependencies
npm install
```

## Security Considerations

1. Dependencies
   - Keep dependencies updated
   - Review security advisories
   - Use `npm audit` regularly

2. Environment Variables
   - Never commit `.env` files
   - Use secure values in production
   - Rotate sensitive credentials

3. Access Control
   - Implement authentication if needed
   - Use HTTPS in production
   - Follow security best practices

## Development Setup

### IDE Configuration
#### VSCode
1. Install recommended extensions:
   - ESLint
   - Prettier
   - Python
   - TypeScript

2. Add workspace settings:
   ```json
   {
     "editor.formatOnSave": true,
     "editor.defaultFormatter": "esbenp.prettier-vscode",
     "python.formatting.provider": "black"
   }
   ```

### Code Style
1. JavaScript/TypeScript:
   - Use ESLint configuration
   - Follow Prettier formatting
   - Use TypeScript strict mode

2. Python:
   - Follow PEP 8 guidelines
   - Use type hints
   - Document functions

## Contributing

### Setup Development Environment
1. Fork the repository
2. Clone your fork
3. Install dependencies
4. Create a feature branch

### Running Tests
```bash
# Run all tests
npm test

# Run specific test file
npm test -- src/converter.test.ts

# Run with coverage
npm test -- --coverage
```

### Code Quality
```bash
# Run linter
npm run lint

# Run type checker
npm run type-check

# Format code
npm run format
```

## Troubleshooting Guide

### Installation Issues

1. Node Version Mismatch
   ```bash
   # Install nvm
   curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.0/install.sh | bash
   
   # Install correct Node version
   nvm install 18
   nvm use 18
   ```

2. Python Dependencies
   ```bash
   # Create virtual environment
   python3 -m venv venv
   source venv/bin/activate
   
   # Install dependencies
   pip install -r requirements.txt
   ```

3. Build Errors
   ```bash
   # Clear build cache
   rm -rf dist
   rm -rf .cache
   
   # Rebuild
   npm run build
   ```

### Runtime Issues

1. Memory Issues
   ```bash
   # Increase Node.js memory limit
   export NODE_OPTIONS=--max_old_space_size=4096
   ```

2. Performance Issues
   - Check system resources
   - Monitor memory usage
   - Profile application

3. Conversion Errors
   - Check SQL syntax
   - Verify supported features
   - Review error messages

## Support

### Getting Help
- GitHub Issues
- Documentation
- Community Forums

### Reporting Bugs
1. Check existing issues
2. Provide reproduction steps
3. Include error messages
4. Share system information

## License
MIT License - See LICENSE file for details