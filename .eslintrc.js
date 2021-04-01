module.exports = {
  parser: '@typescript-eslint/parser',
  parserOptions: {
    project: 'tsconfig.json',
    tsconfigRootDir: __dirname,
    ecmaVersion: 6,
    sourceType: 'module',
    ecmaFeatures: {
      jsx: true
    }
  },
  plugins: [
    'prettier',
    '@typescript-eslint'
  ],
  extends: [
    'plugin:@typescript-eslint/recommended'
  ],
  env: {
    node: true,
    mocha: true,
    es6: true,
    jest: true
  },
  rules: {
    'prefer-rest-params': 'off',
    '@typescript-eslint/no-empty-interface': 'off',
    '@typescript-eslint/no-this-alias': 'off',
    '@typescript-eslint/no-explicit-any': 'off',
    '@typescript-eslint/no-var-requires': 'off',
    '@typescript-eslint/explicit-module-boundary-types': 'off',
    '@typescript-eslint/ban-types': 'off'
  }
};
