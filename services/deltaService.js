// services/deltaService.js

import axios from 'axios';
import 'dotenv/config';

const token = process.env.DELTA_SECURITY_TOKEN;

if (!token) {
    console.error('Токен Delta не найден в .env, запросы к Delta будут пропущены.');
}

// --- ВСПОМОГАТЕЛЬНАЯ ФУНКЦИЯ: Нормализация результата Delta к формату CI_Contragent ---
// Эта функция преобразует ответ от любого из эндпоинтов (company, person, ip) в общий формат.
function normalizeDeltaResult(deltaItem, sourceEndpoint) {
    // Определяем, что пришло из Delta и как это отнести к полям CI_Contragent
    const normalized = {
        // Общие поля
        INN: deltaItem.inn || null,
        OGRN: deltaItem.ogrn || deltaItem.ogrnip || null, // OGRN для юрлиц, OGRNIP для ИП
        KPP: deltaItem.kpp || null,
        NameShort: deltaItem.short_name || deltaItem.name_short || deltaItem.full_name || deltaItem.name_full || deltaItem.fio || null,
        NameFull: deltaItem.full_name || deltaItem.name_full || deltaItem.short_name || deltaItem.name_short || deltaItem.fio || null,
        // PhoneNum: deltaItem.phone || null, // Delta может возвращать телефон в разных местах, нужно уточнить структуру
        // eMail: deltaItem.email || null, // Delta может возвращать email в разных местах
        AddressUr: deltaItem.register_address || deltaItem.residence_address || null, // Уточнить, какой адрес использовать
        UrFiz: null, // Будет установлено ниже
        fIP: null,   // Будет установлено ниже
        // delta_info: deltaItem, // Если нужно сохранить оригинальные данные Delta, раскомментируйте (необязательно)
        // Поля, специфичные для Delta или дополнительные
        status: deltaItem.status || null,
        charter_capital: deltaItem.charter_capital || null,
        main_activity: deltaItem.main_activity || deltaItem.okved || null,
        register_date: deltaItem.register_date || deltaItem.birth_date || null, // Регистрация или дата рождения
        // link: deltaItem.link || null, // Ссылка на карточку
        // company_id: deltaItem.company_id || deltaItem.person_id || deltaItem.id || null, // Унифицированный ID Delta
        type: null, // Будет установлено ниже
        sourceEndpoint: sourceEndpoint, // Для отладки, можно убрать
        // --- ДОБАВЛЯЕМ ИНФОРМАЦИЮ ОБ ИСТОЧНИКЕ ---
        source: 'delta',
        sourceTable: null, // Delta не предоставляет информацию о таблице БД
        baseName: null,    // Delta не предоставляет информацию о BaseName
        // ---
        // Добавим оригинальный ответ как deltaRaw, если нужно
        deltaRaw: deltaItem
    };

    // Определяем тип сущности на основе sourceEndpoint и полей Delta
    if (sourceEndpoint === 'company') {
        normalized.UrFiz = 1; // Условно, юрлицо
        normalized.fIP = 0; // Не ИП
        normalized.type = 'juridical';
    } else if (sourceEndpoint === 'person') {
        normalized.UrFiz = 2; // Условно, физлицо
        normalized.fIP = 0; // Не ИП
        normalized.type = 'physical';
    } else if (sourceEndpoint === 'ip') {
        normalized.UrFiz = 2; // Условно, физлицо (ИП - это физлицо)
        normalized.fIP = 1; // Это ИП
        normalized.type = 'ip';
    } else {
        normalized.type = 'unknown';
    }

    return normalized;
}
// --- ФУНКЦИЯ: Запрос к эндпоинту company ---
async function fetchDeltaCompany(query) {
    if (!token) return [];
    const url = `https://service.deltasecurity.ru/api2/find/company?query=${encodeURIComponent(query)}&token=${token}`;
    console.log(`[Delta API] Выполняем запрос к company: ${url}`);

    try {
        const response = await axios.get(url);
        console.log(`[Delta API] Ответ от company (статус):`, response.data.status_id, response.data.status_text);

        if (response.data.status_id === 1 && Array.isArray(response.data.result)) {
            // Нормализуем каждый элемент результата
            return response.data.result.map(item => normalizeDeltaResult(item, 'company'));
        } else {
            console.log('[Delta API] Company: Нет данных или ошибка в формате ответа.');
            return [];
        }
    } catch (error) {
        console.error('[Delta API] Ошибка при запросе к company:', error.response?.data || error.message);
        return [];
    }
}

// --- ФУНКЦИЯ: Запрос к эндпоинту person ---
async function fetchDeltaPerson(query) {
    if (!token) return [];
    const url = `https://service.deltasecurity.ru/api2/find/person?query=${encodeURIComponent(query)}&token=${token}`;
    console.log(`[Delta API] Выполняем запрос к person: ${url}`);

    try {
        const response = await axios.get(url);
        console.log(`[Delta API] Ответ от person (статус):`, response.data.status_id, response.data.status_text);

        if (response.data.status_id === 1 && Array.isArray(response.data.result)) {
            // Нормализуем каждый элемент результата
            return response.data.result.map(item => normalizeDeltaResult(item, 'person'));
        } else {
            console.log('[Delta API] Person: Нет данных или ошибка в формате ответа.');
            return [];
        }
    } catch (error) {
        console.error('[Delta API] Ошибка при запросе к person:', error.response?.data || error.message);
        return [];
    }
}

// --- ФУНКЦИЯ: Запрос к эндпоинту ip ---
async function fetchDeltaIP(query) {
    if (!token) return [];
    const url = `https://service.deltasecurity.ru/api2/find/ip?query=${encodeURIComponent(query)}&token=${token}`;
    console.log(`[Delta API] Выполняем запрос к ip: ${url}`);

    try {
        const response = await axios.get(url);
        console.log(`[Delta API] Ответ от ip (статус):`, response.data.status_id, response.data.status_text);

        if (response.data.status_id === 1 && Array.isArray(response.data.result)) {
            // Нормализуем каждый элемент результата
            return response.data.result.map(item => normalizeDeltaResult(item, 'ip'));
        } else {
            console.log('[Delta API] IP: Нет данных или ошибка в формате ответа.');
            return [];
        }
    } catch (error) {
        console.error('[Delta API] Ошибка при запросе к ip:', error.response?.data || error.message);
        return [];
    }
}

// --- ОСНОВНАЯ ФУНКЦИЯ: Вызов всех подходящих эндпоинтов ---
async function fetchDeltaData(query) {
    if (!token) {
        console.error('Токен Delta не найден, пропускаем запрос к Delta.');
        return [];
    }

    if (!query || query.trim().length < 3) {
        console.log("Запрос к Delta: пустой или короткий query, пропускаем.");
        return [];
    }

    console.log(`[Delta API] Начинаем поиск по запросу: ${query}`);

    // --- КОНТРОЛЬНЫЙ ОБЗОР ---
    // Почта: Дельта НЕ принимает почту. Значит, не вызываем API по почте.
    // Телефон/Имя/Адрес: Могут быть в query. Дельта принимает их через параметр query в company и ip.
    // ФИО: Могут быть в query. Дельта принимает их через query в company, ip и person.
    // ИНН/ОГРН/ОГРНИП: Могут быть в query. Дельта принимает их через query в company, ip и person.

    // --- ВАЖНО ---
    // Мы вызываем ВСЕ ТРИ эндпоинта с ОДНИМ И ТЕМ ЖЕ query.
    // Это может привести к дубликатам (например, ИНН ИП найдётся и в person, и в ip).
    // Это нормально, дедупликация может происходить позже (в findConnections или на фронтенде).
    // Главное - получить все возможные результаты.

    // Вызываем все три эндпоинта параллельно
    let companyResults = [];
    let personResults = [];
    let ipResults = [];

    try {
        [companyResults, personResults, ipResults] = await Promise.all([
            fetchDeltaCompany(query),
            fetchDeltaPerson(query),
            fetchDeltaIP(query)
        ]);
    } catch (error) {
        // Обработка ошибки, если один из запросов упал, остальные могут быть успешны
        console.error('[Delta API] Ошибка при параллельном запросе к одному или нескольким эндпоинтам:', error);
        // Попытаемся получить результаты, которые успели выполниться
        // Если Promise.all упадёт, то все результаты будут пустыми.
        // Чтобы обработать частичный сбой, можно использовать отдельные Promise, но для простоты оставим так.
        // В реальном приложении может потребоваться более сложная логика.
    }

    // Объединяем результаты
    const deltaResults = [...companyResults, ...personResults, ...ipResults];

    // --- ОПЦИОНАЛЬНО: Простая дедупликация по ИНН и NameShort ---
    // Если нужно избежать дубликатов на этом этапе
    const seen = new Set();
    const uniqueDeltaResults = [];
    deltaResults.forEach(item => {
        const uniqueId = `${item.INN || 'NO_INN'}_${item.NameShort || 'NO_NAME'}`;
        if (!seen.has(uniqueId)) {
            seen.add(uniqueId);
            uniqueDeltaResults.push(item);
        }
    });

    console.log(`[Delta API] Всего нормализованных результатов (до дедупликации): ${deltaResults.length}`);
    console.log(`[Delta API] Всего уникальных нормализованных результатов: ${uniqueDeltaResults.length}`);
    // console.log(`[Delta API] Примеры нормализованных результатов:`, uniqueDeltaResults.slice(0, 2)); // Для отладки

    return uniqueDeltaResults; // Возвращаем объединённый и (опционально) дедуплицированный массив
}

export { fetchDeltaData, fetchDeltaCompany, fetchDeltaPerson, fetchDeltaIP, normalizeDeltaResult };
