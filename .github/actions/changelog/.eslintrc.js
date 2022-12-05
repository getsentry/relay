module.exports = {
  env: {
    browser: true,
    es2021: true
  },
  extends: 'standard',
  plugins: ["prettier"],
  overrides: [
  ],
  parserOptions: {
    ecmaVersion: 'latest',
    sourceType: 'module'
  },
  rules: {
    "prettier/prettier": "error"
  }
}
