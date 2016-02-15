// Nifty project configuration for yyds (hybrid cargo + pnpm).
import { defineConfig } from '@doki-land/nifty';

export default defineConfig({
    format: {
        preset: 'nifty',
        style: {
            indentStyle: 'space',
            indentWidth: 4,
            lineWidth: 100,
            quoteStyle: 'double',
        },
        includes: ['package.json', 'nifty.config.ts', 'scripts/**', 'projects/yyds.ts/**'],
        excludes: [
            '**/node_modules/**',
            '**/dist/**',
            '**/target/**',
            '**/pnpm-lock.yaml',
            '**/*.md',
            'projects/yyds.ts/platforms/**/lib/**',
        ],
    },
    trust: {
        npm: {
            repo: 'yy-database/yyds',
            file: 'release-npm.yml',
            environment: 'NPM_PUBLISH',
        },
    },
    publish: {
        packages: [
            '@yydb/yyds-win32-x64',
            '@yydb/yyds-linux-x64',
            '@yydb/yyds-darwin-x64',
            '@yydb/yyds-darwin-arm64',
            '@yydb/yyds',
            '@yydb/redis',
            '@yydb/mysql',
            '@yydb/postgresql',
        ],
    },
});
