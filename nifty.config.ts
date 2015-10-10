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
            '@yydb/yyds-win32-x64',
            '@yydb/yyds-linux-x64',
            '@yydb/yyds-darwin-x64',
            '@yydb/yyds-darwin-arm64',
            '@yydb/yyds',
            '@yydb/redis',
            '@yydb/mysql',
            '@yydb/postgresql',
            '@yydb/sqlite',
        ],
    },
});
