import whiskey from '@whiskeysockets/eslint-config'
import prettierRec from 'eslint-plugin-prettier/recommended'

export default [
	{
		ignores: [
			'lib/**',
			'WAProto/**',
			'proto-extract/**',
			'Example/**',
			'docs/**',
			'src/Tests/**',
			'coverage/**'
		]
	},
	...whiskey,
	prettierRec,
	{
		files: ['src/**/*.ts'],
		languageOptions: { parserOptions: { project: './tsconfig.json' } },
		rules: {
			camelcase: 'off',
			indent: 'off',
			'no-restricted-syntax': 'off',
			'keyword-spacing': 'off',
			'implicit-arrow-linebreak': 'off',
			'space-before-function-paren': ['error', { anonymous: 'always', named: 'never', asyncArrow: 'always' }],
			'@typescript-eslint/no-unused-vars': ['error', { caughtErrors: 'none' }],
			'prettier/prettier': 'off',
			'padding-line-between-statements': 'off',
			'simple-import-sort/imports': 'off'
		}
	}
]
