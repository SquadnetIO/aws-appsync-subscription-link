import json from '@rollup/plugin-json';
import commonjs from '@rollup/plugin-commonjs';
import typescript from '@rollup/plugin-typescript';
import multiInput from 'rollup-plugin-multi-input';
import svg from 'rollup-plugin-svg';
import jsx from 'acorn-jsx';
import pkg from './package.json';

export default [
  {
    input: ['src/index.ts', 'src/**/*.tsx', 'src/**/*.ts'],
    output: {
      dir: './dist',
      format: 'esm',
      sourcemap: true
    },
    plugins: [
      commonjs(),
      typescript({
        declarationDir: "dist",
      }),
      multiInput({ relative: 'src/' }),
      json(),
      svg(),
    ],
    acornInjectPlugins: [jsx()],
    external: [
      ...Object.keys(pkg.dependencies),
      ...Object.keys(pkg.peerDependencies),
      ...Object.keys(pkg.devDependencies),
      'styled-components/native',
      'react-is',
      'lodash'
    ]
  }
];
