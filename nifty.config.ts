// Nifty project configuration for yyds (hybrid cargo + pnpm).
import { defineConfig } from '@doki-land/nifty';

export default defineConfig({
    trust: {
        npm: {
            repo: 'yy-database/yyds',
            file: 'release-npm.yml',
            environment: 'NPM_PUBLISH',
        },
    },
    publish: {
        packages: [
            '@yyds/yyds-win32-x64',
            '@yyds/yyds-linux-x64',
            '@yyds/yyds-darwin-x64',
            '@yyds/yyds-darwin-arm64',
            '@yyds/yyds-unknown-wasm32',
            '@yyds/yyds',
            '@yyds/redis',
            '@yyds/mysql',
            '@yyds/postgresql',
            '@yyds/sqlite',
        ],
    },
});
