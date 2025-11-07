// server.js
require('dotenv').config(); // Загружаем переменные из .env
const express = require('express');
const sql = require('mssql');
const path = require('path');
const { findConnectionsByEmail, findConnectionsByPhone } = require('./find_connections'); // Импортируем функцию

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


// API: Поиск по запросу (только с _test таблицами)
app.post('/api/search', async (req, res) => {
    const { query } = req.body;
    if (!query || query.trim().length < 3) {
        return res.json({ juridical: [], physical: [], ip: [] });
    }

    try {
        await sql.connect(config);

        const request = new sql.Request();
        request.input('query', sql.VarChar, `%${query}%`);

        const searchQuery = `
            SELECT TOP 20
                INN, OGRN, NameShort, NameFull, AddressUr, AddressUFakt as AddressUFakt, PhoneNum, eMail, UrFiz, fIP, 'juridical' as type,
                UNID as UNID
            FROM CI_Contragent_test
            WHERE (INN LIKE @query OR NameShort LIKE @query OR NameFull LIKE @query OR PhoneNum LIKE @query OR eMail LIKE @query OR AddressUr LIKE @query OR AddressUFakt LIKE @query)
              AND UrFiz = 1 AND fIP = 0

            UNION ALL

            SELECT TOP 20
                INN, OGRN, NameShort, NameFull, AddressUr, AddressUFakt as AddressUFakt, PhoneNum, eMail, UrFiz, fIP, 'ip' as type,
                UNID as UNID
            FROM CI_Contragent_test
            WHERE (INN LIKE @query OR NameShort LIKE @query OR NameFull LIKE @query OR PhoneNum LIKE @query OR eMail LIKE @query OR AddressUr LIKE @query OR AddressUFakt LIKE @query)
              AND fIP = 1

            UNION ALL

            SELECT TOP 20
                INN, NULL as OGRN, NameShort, NameFull, AddressUr, AddressUFakt as AddressUFakt, PhoneNum, eMail, UrFiz, fIP, 'physical' as type,
                UNID as UNID
            FROM CI_Contragent_test
            WHERE (INN LIKE @query OR NameShort LIKE @query OR NameFull LIKE @query OR PhoneNum LIKE @query OR eMail LIKE @query OR AddressUr LIKE @query OR AddressUFakt LIKE @query)
              AND UrFiz = 2 AND fIP = 0

            UNION ALL

            SELECT TOP 20
                NULL as INN, NULL as OGRN, fzFIO as NameShort, NULL as NameFull, fzAddress as AddressUr, fzAddressF as AddressUFakt, fzPhoneM as PhoneNum, fzMail as eMail, NULL as UrFiz, NULL as fIP, 'physical' as type,
                fzUID as fzUID
            FROM CI_Employees_test
            WHERE (fzFIO LIKE @query OR fzPhoneM LIKE @query OR fzMail LIKE @query OR fzAddress LIKE @query OR fzAddressF LIKE @query)
        `;

        const result = await request.query(searchQuery);
        const allResults = result.recordset;

        const juridical = allResults.filter(item => item.type === 'juridical');
        const ip = allResults.filter(item => item.type === 'ip');
        const physical = allResults.filter(item => item.type === 'physical');

        console.log(`Найдено юрлиц: ${juridical.length}, ИП: ${ip.length}, физлиц: ${physical.length}`);

        // --- ИНТЕГРАЦИЯ СВЯЗЕЙ ЧЕРЕЗ МОДЕЛЬ ---
        const allTargetEntities = [...juridical, ...ip, ...physical];
        console.log(`Найдено ${allTargetEntities.length} сущностей для поиска связей (до фильтрации по email/phone).`);

        // --- НОВЫЙ БЛОК: Поиск связей по email ---
        const entitiesWithMailAndKey = allTargetEntities.filter(entity => {
             const entityKey = entity.UNID || entity.fzUID || entity.cpUID;
             return entityKey && entity.eMail && entity.eMail.trim() !== '';
        });
        console.log(`Найдено ${entitiesWithMailAndKey.length} сущностей с ключом и email для поиска связей.`);
        const emailConnectionsMap = await findConnectionsByEmail(entitiesWithMailAndKey, request);

        // --- НОВЫЙ БЛОК: Поиск связей по телефону ---
        const entitiesWithPhoneAndKey = allTargetEntities.filter(entity => {
             const entityKey = entity.UNID || entity.fzUID || entity.cpUID;
             // Проверяем наличие хотя бы одного из телефонных полей
             const hasPhone = entity.PhoneNum || entity.fzPhoneM || entity.cpPhoneMob || entity.cpPhoneWork;
             return entityKey && hasPhone;
        });
        console.log(`Найдено ${entitiesWithPhoneAndKey.length} сущностей с ключом и телефоном для поиска связей.`);
        const phoneConnectionsMap = await findConnectionsByPhone(entitiesWithPhoneAndKey, request); // Вызываем новую функцию

        // Привязываем связи к НАЙДЕННЫМ сущностям (allResults)
        allResults.forEach(entity => {
            const entityKey = entity.UNID || entity.fzUID || entity.cpUID;
            const entityConnections = [];

            // Добавляем связи по email
            if (entityKey && emailConnectionsMap.has(entityKey)) {
                const emailConnections = emailConnectionsMap.get(entityKey) || {};
                for (const [emailValue, connections] of Object.entries(emailConnections)) {
                    entityConnections.push({
                        contact: emailValue,
                        type: 'contact',
                        subtype: 'email',
                        connections: connections
                    });
                }
            }

            // Добавляем связи по телефону
            if (entityKey && phoneConnectionsMap.has(entityKey)) {
                const phoneConnections = phoneConnectionsMap.get(entityKey) || {};
                for (const [phoneValue, connections] of Object.entries(phoneConnections)) {
                    entityConnections.push({
                        contact: phoneValue,
                        type: 'contact',
                        subtype: 'phone',
                        connections: connections
                    });
                }
            }

            entity.connections = entityConnections;
            entity.connectionsCount = entity.connections.length;
        });
        // --- КОНЕЦ ИНТЕГРАЦИИ ---

        res.json({
            juridical: juridical,
            physical: physical,
            ip: ip
        });

    } catch (err) {
        console.error('Ошибка при выполнении запроса к БД:', err);
        res.status(500).json({ error: err.message });
    }
});
app.listen(3000, () => {
    console.log('✅ Сервер запущен: http://localhost:3000');
});