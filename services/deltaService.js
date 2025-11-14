// services/deltaService.js

import axios from 'axios';
import 'dotenv/config';

// --- КОНСТАНТЫ И КОНФИГУРАЦИЯ ---
const DELTA_CONFIG = {
    BASE_URL: 'https://service.deltasecurity.ru/api2/find',
    TOKEN: process.env.DELTA_SECURITY_TOKEN,
    ENDPOINTS: {
        COMPANY: 'company',
        PERSON: 'person', 
        IP: 'ip'
    },
    STATUS: {
        SUCCESS: 1,
        NO_RESULTS: 2
    }
};

// --- ВАЛИДАЦИЯ ---
function validateToken() {
    if (!DELTA_CONFIG.TOKEN) {
        console.error('❌ Токен Delta не найден в .env, запросы к Delta будут пропущены.');
        return false;
    }
    return true;
}

function validateQuery(query) {
    if (!query || query.trim().length < 3) {
        console.log("⏩ Запрос к Delta: пустой или короткий query, пропускаем.");
        return false;
    }
    return true;
}

// --- УТИЛИТЫ ---
function buildDeltaUrl(endpoint, params = {}) {
    const searchParams = new URLSearchParams({
        ...params,
        token: DELTA_CONFIG.TOKEN
    });
    return `${DELTA_CONFIG.BASE_URL}/${endpoint}?${searchParams.toString()}`;
}

function isHtmlResponse(response) {
    return response.headers['content-type']?.includes('text/html');
}

function createUniqueId(item) {
    return `${item.INN || 'NO_INN'}_${item.NameShort || 'NO_NAME'}`;
}

// --- НОРМАЛИЗАЦИЯ ДАННЫХ ---
function normalizeDeltaResult(deltaItem, sourceEndpoint) {
    const entityTypeConfig = getEntityTypeConfig(sourceEndpoint);
    
    const normalized = {
        // Основные идентификаторы
        INN: deltaItem.inn || null,
        OGRN: deltaItem.ogrn || deltaItem.ogrnip || null,
        KPP: deltaItem.kpp || null,
        
        // Наименования
        NameShort: getBestName(deltaItem, 'short'),
        NameFull: getBestName(deltaItem, 'full'),
        
        // Адреса
        AddressUr: deltaItem.register_address || deltaItem.residence_address || null,
        
        // Типизация
        UrFiz: entityTypeConfig.urFiz,
        fIP: entityTypeConfig.fIP,
        type: entityTypeConfig.type,
        
        // Дополнительная информация
        status: deltaItem.status || null,
        charter_capital: deltaItem.charter_capital || null,
        main_activity: deltaItem.main_activity || deltaItem.okved || null,
        register_date: deltaItem.register_date || deltaItem.birth_date || null,
        
        // Мета-информация
        source: 'delta',
        sourceEndpoint: sourceEndpoint,
        sourceTable: null,
        baseName: null,
        deltaRaw: deltaItem
    };

    return normalized;
}

function getEntityTypeConfig(sourceEndpoint) {
    const configs = {
        company: { urFiz: 1, fIP: 0, type: 'juridical' },
        person: { urFiz: 2, fIP: 0, type: 'physical' },
        ip: { urFiz: 2, fIP: 1, type: 'ip' }
    };
    
    return configs[sourceEndpoint] || { urFiz: null, fIP: null, type: 'unknown' };
}

function getBestName(deltaItem, nameType) {
    const nameVariants = {
        short: [
            deltaItem.short_name,
            deltaItem.name_short, 
            deltaItem.fio,
            deltaItem.full_name,
            deltaItem.name_full
        ],
        full: [
            deltaItem.full_name,
            deltaItem.name_full,
            deltaItem.short_name,
            deltaItem.name_short,
            deltaItem.fio
        ]
    };
    
    return nameVariants[nameType].find(name => name) || null;
}

// --- ОБРАБОТЧИКИ ОТВЕТОВ ---
function handleDeltaResponse(response, endpoint) {
    console.log(`[Delta API] Ответ от ${endpoint}:`, response.data.status_id, response.data.status_text);

    if (response.data.status_id === DELTA_CONFIG.STATUS.SUCCESS && 
        Array.isArray(response.data.result)) {
        return response.data.result.map(item => normalizeDeltaResult(item, endpoint));
    }
    
    console.log(`[Delta API] ${endpoint}: Нет данных или ошибка в формате ответа.`);
    return [];
}

function handleDeltaError(error, endpoint, url) {
    if (error.response && isHtmlResponse(error.response)) {
        console.error(`[Delta API] Ошибка ${endpoint}: получен HTML-ответ. URL: ${url}`);
        console.error(`[Delta API] Тело ошибки: ${error.response.data.substring(0, 200)}...`);
    } else {
        console.error(`[Delta API] Ошибка ${endpoint}:`, error.response?.data || error.message);
    }
    return [];
}

// --- API ЗАПРОСЫ ---
async function makeDeltaRequest(endpoint, params = {}) {
    if (!validateToken()) return [];
    
    const url = buildDeltaUrl(endpoint, params);
    console.log(`[Delta API] Выполняем запрос к ${endpoint}: ${url}`);

    try {
        const response = await axios.get(url);
        
        if (isHtmlResponse(response)) {
            console.error(`[Delta API] ${endpoint}: получен HTML-ответ. URL: ${url}`);
            return [];
        }
        
        return handleDeltaResponse(response, endpoint);
    } catch (error) {
        return handleDeltaError(error, endpoint, url);
    }
}

// --- СПЕЦИФИЧНЫЕ ЗАПРОСЫ ---
async function fetchDeltaCompany(query) {
    return makeDeltaRequest(DELTA_CONFIG.ENDPOINTS.COMPANY, { query });
}

async function fetchDeltaPerson(query) {
    return makeDeltaRequest(DELTA_CONFIG.ENDPOINTS.PERSON, { query });
}

async function fetchDeltaIP(query) {
    const params = buildIPSearchParams(query);
    return makeDeltaRequest(DELTA_CONFIG.ENDPOINTS.IP, params);
}

function buildIPSearchParams(query) {
    const isINN = /^\d{10,12}$/.test(query);
    const isOGRNIP = /^\d{15}$/.test(query);
    
    if (isOGRNIP) return { ogrnip: query };
    if (isINN) return { inn: query };
    return { query };
}

// --- ОСНОВНАЯ ФУНКЦИЯ ---
async function fetchDeltaData(query) {
    if (!validateToken() || !validateQuery(query)) return [];

    console.log(`[Delta API] 🔍 Начинаем поиск по запросу: ${query}`);

    try {
        const [companyResults, personResults, ipResults] = await Promise.all([
            fetchDeltaCompany(query),
            fetchDeltaPerson(query),
            fetchDeltaIP(query)
        ]);

        const allResults = [...companyResults, ...personResults, ...ipResults];
        const uniqueResults = deduplicateResults(allResults);

        logSearchResults(allResults, uniqueResults);
        return uniqueResults;

    } catch (error) {
        console.error('[Delta API] ❌ Ошибка при параллельном запросе:', error);
        return [];
    }
}

function deduplicateResults(results) {
    const seen = new Set();
    return results.filter(item => {
        const uniqueId = createUniqueId(item);
        const isDuplicate = seen.has(uniqueId);
        seen.add(uniqueId);
        return !isDuplicate;
    });
}

function logSearchResults(allResults, uniqueResults) {
    console.log(`[Delta API] 📊 Результаты поиска:`);
    console.log(`[Delta API]   Всего найдено: ${allResults.length}`);
    console.log(`[Delta API]   После дедупликации: ${uniqueResults.length}`);
    
    if (uniqueResults.length > 0) {
        console.log(`[Delta API]   Типы сущностей:`, {
            juridical: uniqueResults.filter(r => r.type === 'juridical').length,
            physical: uniqueResults.filter(r => r.type === 'physical').length,
            ip: uniqueResults.filter(r => r.type === 'ip').length
        });
    }
}

export { 
    fetchDeltaData, 
    fetchDeltaCompany, 
    fetchDeltaPerson, 
    fetchDeltaIP, 
    normalizeDeltaResult 
};