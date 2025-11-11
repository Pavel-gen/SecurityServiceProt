// eslint.config.mjs
import js from "@eslint/js";
import globals from "globals";
// import tseslint from "typescript-eslint"; // Убираем, если не используем TypeScript
import { defineConfig } from "eslint/config";

export default defineConfig([
  {
    // Применяем к файлам .js, .mjs, .cjs
    files: ["**/*.{js,mjs,cjs}"],
    // Используем рекомендуемые правила ESLint для JavaScript
    ...js.configs.recommended,
    // Настройки языка
    languageOptions: {
      // Указываем версию ECMAScript (по умолчанию часто latest)
      ecmaVersion: "latest",
      // Указываем глобальные переменные
      // ИСПРАВЛЕНО: используем spread operator (...) для вставки объекта globals.node
      globals: {
        // globals.browser, // Убираем, если не нужно, или также используем spread operator, если нужно
        ...globals.node, // <-- Правильный способ: распаковываем объект globals.node
        // Можно добавить и другие глобальные переменные, специфичные для вашего проекта
        // Например: myCustomGlobal: "readonly"
      },
      // Указываем парсер (по умолчанию используется встроенный)
      parserOptions: {
        ecmaVersion: "latest",
        sourceType: "module", // или "script", если используете CommonJS (require/module.exports)
      },
    },
  },
]);