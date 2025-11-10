// server.js
require('dotenv').config(); // Загружаем переменные из .env
const express = require('express');
const sql = require('mssql');
const path = require('path');
const { findConnectionsByEmail, findConnectionsByPhone, findConnectionsByINN, findConnections} = require('./find_connections'); // Импортируем функцию
const axios = require('axios');
const { filterEntitiesByCompleteness } = require('./filters');
const { fetchLocalData } = require('./findLocalData');

const app = express();

app.use(express.static('.')); // раздаём всё из корня
app.use(express.json());

// Конфиг БД
const config = {
    user: process.env.DB_USER, // Берём из .env
    password: process.env.DB_PASSWORD, // Берём из .env
    server: process.env.DB_SERVER, // Берём из .env
    port: parseInt(process.env.DB_PORT), // Берём из .env, преобразуем в число
    database: process.env.DB_NAME, // Берём из .env
    options: { encrypt: process.env.DB_ENCRYPT === 'true' } // Берём из .env, преобразуем в boolean
};

function filterUniqueConnections(connectionsArray) {
    const seen = new Set();
    const filtered = [];

    connectionsArray.forEach(conn => {
        // Создаем уникальный идентификатор на основе INN и NameShort
        const uniqueId = `${conn.connectedEntity.INN}_${conn.connectedEntity.NameShort}`;
        if (!seen.has(uniqueId)) {
            seen.add(uniqueId);
            filtered.push(conn);
        } else {
            console.log(`Фильтруем дубликат: ${uniqueId}`);
        }
    });

    return filtered;
}

app.get('/api/delta-search', async (req, res) => {
    const token = process.env.DELTA_SECURITY_TOKEN;
    if (!token) {
        console.error('Токен Дельтабезопасности не найден в .env');
        return res.status(500).json({ error: 'Токен Дельтабезопасности не настроен' });
    }

    // Получаем параметры поиска из query string
    const { inn, ogrn, company_name, query } = req.query; // Добавим другие возможные параметры

    // Проверяем, задан ли хотя бы один параметр поиска
    if (!inn && !ogrn && !company_name && !query) {
        return res.status(400).json({ error: 'Требуется указать хотя бы один параметр: inn, ogrn, company_name или query' });
    }

    // Формируем URL с параметрами
    // Используем 'query' как универсальный параметр, если другие не заданы
    let searchParam = query;
    if (!searchParam) {
        // Приоритет: query > inn > ogrn > company_name
        if (inn) searchParam = inn;
        else if (ogrn) searchParam = ogrn;
        else if (company_name) searchParam = company_name;
    }

    const url = `https://service.deltasecurity.ru/api2/find/company?query=${encodeURIComponent(searchParam)}&token=${token}`;

    console.log(`Выполняем запрос к Дельтабезопасности: ${url}`);

    try {
        const response = await axios.get(url);
        console.log('Ответ от Дельтабезопасности (статус):', response.data.status_id, response.data.status_text);
        // console.log('Ответ от Дельтабезопасности (детали):', response.data.result); // Раскомментируй для подробного лога

        // Возвращаем ответ от Дельтабезопасности клиенту
        res.json(response.data);
    } catch (error) {
        console.error('Ошибка при запросе к Дельтабезопасности:', error.response?.data || error.message);
        res.status(500).json({ error: 'Ошибка при запросе к Дельтабезопасности', details: error.message });
    }
});

app.get('/test-delta', async (req, res) => {
    const token = process.env.DELTA_SECURITY_TOKEN;

    if (!token) {
        console.error('Токен Дельтабезопасности не найден в .env');
        return res.status(500).json({ error: 'Токен Дельтабезопасности не настроен' });
    }

    const innToSearch = '9721032982'; // Тестируемый ИНН
    const url = `https://service.deltasecurity.ru/api2/find/person?query=${innToSearch}&token=${token}`;

    console.log(`Выполняем запрос к Дельтабезопасности: ${url}`);

    try {
        const response = await axios.get(url);
        console.log('Ответ от Дельтабезопасности:', response.data);
        res.json(response.data); // Отправляем ответ клиенту (или в консоль, если только тест)
    } catch (error) {
        console.error('Ошибка при запросе к Дельтабезопасности:', error.response?.data || error.message);
        res.status(500).json({ error: 'Ошибка при запросе к Дельтабезопасности', details: error.message });
    }
});
// -
// server.js
// ... (ваш существующий код до app.post('/api/search', ...) ) ...

// --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---

// Функция для выполнения запроса к Delta Безопасности
async function fetchDeltaData(query) {
    let deltaResults = [];
    const token = process.env.DELTA_SECURITY_TOKEN;

    if (!token) {
        console.error('Токен Delta не найден, пропускаем запрос к Delta.');
        return deltaResults;
    }

    const deltaUrl = `https://service.deltasecurity.ru/api2/find/company?query=${encodeURIComponent(query)}&token=${token}`;
    console.log(`Выполняем запрос к Дельтабезопасности: ${deltaUrl}`);

    try {
        const deltaResponse = await axios.get(deltaUrl);
        console.log('Ответ от Дельтабезопасности (статус):', deltaResponse.data.status_id, deltaResponse.data.status_text);

        if (deltaResponse.data.status_id === 1 && Array.isArray(deltaResponse.data.result)) {
            deltaResults = deltaResponse.data.result;
            console.log(`Найдено в Delta: ${deltaResults.length} сущностей`);
        } else {
            console.log('Delta: Нет данных или ошибка в формате ответа.');
        }
    } catch (error) {
        console.error('Ошибка при запросе к Дельтабезопасности:', error.response?.data || error.message);
    }

    return deltaResults;
}

// Функция для объединения локальных данных с данными из Delta
function mergeLocalAndDelta(localEntities, deltaResults) {
    const deltaMapByINN = new Map();
    deltaResults.forEach(item => {
        if (item.inn) {
            deltaMapByINN.set(item.inn, item);
        }
    });

    const mergedResults = [];
    // Отдельно будем накапливать сущности, которые нужно будет проверить на объединение с Delta.
    // Это должны быть сущности, представляющие юр/физ лиц, а не вспомогательные (prevwork, contacts).
    const entitiesToMerge = [];
    const otherLocalEntities = []; // Остальные, которые не нужно объединять по ИНН (например, prevwork, contacts)

    localEntities.forEach(localEntity => {
        // Определяем, нужно ли объединять эту конкретную сущность по ИНН с Delta.
        // Обычно это делается для сущностей, представляющих юр/физ лиц: contragent, employee, contperson, person.
        // Не объединяем prevwork, contact и т.д. напрямую по ИНН с юрлицом из Delta.
        // Проверяем наличие ключевых полей, характерных для "основной" сущности.
        // UNID - характерен для CI_Contragent, CF_Persons
        // fzUID - характерен для CI_Employees
        // cpUID - характерен для CI_ContPersons
        // PersonUNID сам по себе не гарантирует, что это юр/физ лицо (это внешний ключ в prevwork, contacts)
        if ((localEntity.UNID || localEntity.fzUID || localEntity.cpUID) && localEntity.INN) {
             // Это потенциально сущность юр/физ лица, которую можно объединить
             entitiesToMerge.push(localEntity);
        } else {
             // Это, скорее всего, вспомогательная сущность (prevwork, contact), добавляем отдельно
             otherLocalEntities.push(localEntity);
        }
    });

    // Объединяем только основные сущности (contragent, employee, contperson, person) с Delta
    entitiesToMerge.forEach(localEntity => {
        const mergedEntity = { ...localEntity };

        if (localEntity.INN && deltaMapByINN.has(localEntity.INN)) {
            const deltaData = deltaMapByINN.get(localEntity.INN);

            // --- МАППИНГ ПОЛЕЙ ИЗ DELTA ---
            // Приоритет: данные из Delta заменяют локальные, если они есть и не пусты
            if (deltaData.full_name && deltaData.full_name.trim() !== '') mergedEntity.NameFull = deltaData.full_name;
            if (deltaData.short_name && deltaData.short_name.trim() !== '') mergedEntity.NameShort = deltaData.short_name;
            if (deltaData.status && deltaData.status.trim() !== '') mergedEntity.status_from_delta = deltaData.status;
            if (deltaData.register_address && deltaData.register_address.trim() !== '') mergedEntity.AddressUr = deltaData.register_address;
            if (deltaData.charter_capital !== undefined && deltaData.charter_capital !== null) mergedEntity.charter_capital = deltaData.charter_capital;
            if (deltaData.main_activity && deltaData.main_activity.trim() !== '') mergedEntity.main_activity = deltaData.main_activity;
            if (deltaData.register_date && deltaData.register_date.trim() !== '') mergedEntity.register_date = deltaData.register_date;
            if (deltaData.register_type && deltaData.register_type.trim() !== '') mergedEntity.register_type = deltaData.register_type;
            // --- МАППИНГ ТЕЛЕФОНОВ ИЗ DELTA ---
            if (deltaData.fnsAdress_kpp?.phone && deltaData.fnsAdress_kpp.phone.trim() !== '') {
                // Приоритет: если в deltaData есть прямое поле phone, используем его, иначе телефон ИФНС
                mergedEntity.PhoneNum = deltaData.phone || deltaData.fnsAdress_kpp.phone;
            } else if (deltaData.fnsAdress_inn?.phone && deltaData.fnsAdress_inn.phone.trim() !== '') {
                 mergedEntity.PhoneNum = deltaData.phone || deltaData.fnsAdress_inn.phone;
            }
            // --- КОНТЕКСТ: delta_info ---
            // УБРАНО: mergedEntity.delta_info = deltaData; // <-- Закомментировано/удалено

            deltaMapByINN.delete(localEntity.INN); // Удаляем, чтобы не добавлять как отдельную сущность ниже
        }
        mergedResults.push(mergedEntity);
    });

    // Добавляем вспомогательные сущности (prevwork, contacts) без объединения
    mergedResults.push(...otherLocalEntities);

    // Все оставшиеся сущности из Delta, которые не были сопоставлены с *основными* локальными сущностями
    const unmatchedDeltaResults = Array.from(deltaMapByINN.values());

    return { mergedResults, unmatchedDeltaResults };
}

// Функция для поиска связей


// --- ОСНОВНОЙ ЭНДПОИНТ ---
app.post('/api/search', async (req, res) => {
    const { query } = req.body;
    if (!query || query.trim().length < 3) {
        return res.json({ juridical: [], physical: [], ip: [], delta_results: [] });
    }

    try {
        // Шаг 1: Получить локальные данные
        const { localResults, allTargetEntitiesForConnections } = await fetchLocalData(query);

        // Шаг 2: Получить данные из Delta
        const deltaResults = await fetchDeltaData(query);

        // console.log("Delta returned: ", deltaResults);

        // Шаг 3: Объединить локальные данные с Delta
        const { mergedResults, unmatchedDeltaResults } = mergeLocalAndDelta(allTargetEntitiesForConnections, deltaResults);

        // console.log('Merged Results', mergedResults, unmatchedDeltaResults);

        // Шаг 4: Найти связи для объединённых результатов
        const resultsWithConnections = await findConnections([...mergedResults, ...unmatchedDeltaResults]);

        // console.log("RESULTS WITH CONNECTIONS: ", resultsWithConnections);

        // Шаг 5: Сгруппировать объединённые результаты с связями
        const updatedJuridical = resultsWithConnections.filter(item => item.type === 'juridical');
        const updatedIp = resultsWithConnections.filter(item => item.type === 'ip');
        const updatedPhysical = resultsWithConnections.filter(item => item.type === 'physical');

        // Отправить ответ
        res.json({
            juridical: updatedJuridical,
            physical: updatedPhysical,
            ip: updatedIp,
            delta_results: unmatchedDeltaResults // Те, что только в Delta
        });

    } catch (error) {
        console.error('Неожиданная ошибка в /api/search:', error);
        res.status(500).json({ error: 'Внутренняя ошибка сервера' });
    }
});

// ... (остальные маршруты, включая /test-delta и /api/delta-search) ...

app.listen(3000, () => {
    console.log('✅ Сервер запущен: http://localhost:3000');
});