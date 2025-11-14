/* eslint-disable no-unused-vars */
// find_connections.js
import { findConnectionsByEmail } from "./connectionByEmail.js";
import { findConnectionsByINN } from "./connectionByINNService.js";
import { findConnectionsByPhone } from "./connectionByPhoneService.js";
import { getEntityKey } from '../utils/helper.js';

const SEARCH_CONFIGS = [
    {
        name: 'email',
        filter: entity => {
            const entityKey = getEntityKey(entity);
            if (!entityKey) return false;
            
            const hasEmail = (entity.eMail && entity.eMail.trim() !== '') ||
                            (entity.cpMail && entity.cpMail.trim() !== '') ||
                            (entity.fzMail && entity.fzMail.trim() !== '') ||
                            (entity.Contact && entity.Contact.includes('@'));
            
            return hasEmail;
        },
        findFunction: findConnectionsByEmail,
        type: 'contact',
        subtype: 'email'
    },
    {
        name: 'inn',
        filter: entity => {
            const entityKey = getEntityKey(entity);
            if (!entityKey) return false;
            
            const hasAnyINN = (entity.INN && entity.INN.trim() !== '') ||
                             (entity.phOrgINN && entity.phOrgINN.trim() !== '') ||
                             (entity.fzINN && entity.fzINN.trim() !== '') ||
                             (entity.conINN && entity.conINN.trim() !== '');
            
            return hasAnyINN;
        },
        findFunction: findConnectionsByINN,
        type: 'inn',
        subtype: 'inn_match'
    }
];

async function findConnections(entities) {
    console.log("🚀 ========== ЗАПУСК FIND_CONNECTIONS ==========");
    console.log("📥 Входные сущности:", entities.map(e => ({
        type: e.type,
        key: getEntityKey(e),
        INN: e.INN,
        email: e.eMail || e.Contact
    })));

    // --- НОРМАЛИЗАЦИЯ ВХОДНЫХ ДАННЫХ ---
    console.log("🔄 Нормализация сущностей...");
    const normalizedEntities = entities.map(normalizeEntityForConnections);
    
    // --- МНОГОУРОВНЕВЫЙ ПОИСК СВЯЗЕЙ ---
    console.log("🔄 ========== МНОГОУРОВНЕВЫЙ ПОИСК СВЯЗЕЙ ==========");
    const allResults = await performMultiLevelSearch(normalizedEntities);
    
    console.log(`✅ Поиск завершен: ${allResults.length} сущностей с связями`);
    return allResults;
}

// --- НОВАЯ ФУНКЦИЯ МНОГОУРОВНЕВОГО ПОИСКА ---
async function performMultiLevelSearch(initialEntities) {
    console.log("🔄 === НАЧАЛО МНОГОУРОВНЕВОГО ПОИСКА ===");
    
    const allProcessedKeys = new Set();
    const allResults = [];
    let currentLevelEntities = [...initialEntities];
    let level = 1;
    const MAX_LEVELS = 2; // Максимум 2 уровня (прямые + косвенные связи)

    // Добавляем исходные сущности в обработанные
    initialEntities.forEach(entity => {
        const key = getEntityKey(entity);
        if (key) allProcessedKeys.add(key);
    });

    while (currentLevelEntities.length > 0 && level <= MAX_LEVELS) {
        console.log(`\n📊 === УРОВЕНЬ ${level} ===`);
        console.log(`📊 Обрабатываем ${currentLevelEntities.length} сущностей`);
        
        // Выполняем поиск для текущего уровня
        const levelResults = await performLevelSearch(currentLevelEntities, level);
        
        // Добавляем результаты текущего уровня
        allResults.push(...levelResults);
        
        // Подготавливаем сущности для следующего уровня
        const nextLevelEntities = await prepareNextLevelEntities(
            levelResults, 
            allProcessedKeys, 
            level
        );
        
        console.log(`📊 Уровень ${level} завершен: ${levelResults.length} результатов`);
        console.log(`📊 Следующий уровень: ${nextLevelEntities.length} новых сущностей`);
        
        currentLevelEntities = nextLevelEntities;
        level++;
    }

    console.log(`\n✅ Многоуровневый поиск завершен: ${allResults.length} сущностей, ${level-1} уровней`);
    return allResults;
}

async function performLevelSearch(entities, level) {
    console.log(`🔍 === ПОИСК НА УРОВНЕ ${level} ===`);
    
    // Подготовка данных для поиска
    const searchData = prepareSearchData(entities);
    
    // Выполняем поиск связей
    const connectionsResults = await executeConnectionsSearch(searchData);
    
    // Строим результаты ТОЛЬКО с прямыми связями (без добавления найденных сущностей)
    const levelResults = buildLevelResults(entities, searchData, connectionsResults, level);
    
    return levelResults;
}

function buildLevelResults(entities, searchData, connectionsResults, level) {
    console.log(`🔧 === СОЗДАНИЕ РЕЗУЛЬТАТОВ УРОВНЯ ${level} ===`);
    
    const results = entities.map(entity => {
        const entityKey = getEntityKey(entity);
        let entityConnections = [];
        
        SEARCH_CONFIGS.forEach(config => {
            const connectionsMap = connectionsResults[config.name];
            if (entityKey && connectionsMap && connectionsMap.has(entityKey)) {
                const connections = connectionsMap.get(entityKey);
                
                Object.entries(connections).forEach(([contactKey, connectionGroup]) => {
                    const connectionsArray = connectionGroup.connections || [];
                    
                    entityConnections.push({
                        contact: contactKey,
                        type: config.type,
                        subtype: config.subtype,
                        connections: connectionsArray,
                        searchLevel: level // Добавляем информацию об уровне поиска
                    });
                });
            }
        });
        
        return {
            ...entity,
            connections: entityConnections,
            connectionsCount: entityConnections.reduce((sum, conn) => sum + conn.connections.length, 0),
            searchLevel: level
        };
    });
    
    console.log(`✅ Уровень ${level}: ${results.length} сущностей с связями`);
    return results;
}

async function prepareNextLevelEntities(levelResults, allProcessedKeys, currentLevel) {
    console.log(`🔍 === ПОДГОТОВКА СУЩНОСТЕЙ ДЛЯ УРОВНЯ ${currentLevel + 1} ===`);
    
    const nextLevelEntities = new Map();
    let newEntitiesFound = 0;

    // Ищем новые сущности в связях текущего уровня
    levelResults.forEach((entity, index) => {
        console.log(`\n🔍 Анализ сущности ${index + 1}/${levelResults.length}: ${getEntityKey(entity)}`);
        
        if (entity.connections && Array.isArray(entity.connections)) {
            entity.connections.forEach((connectionGroup, groupIndex) => {
                console.log(`  📂 Группа связей ${groupIndex + 1}: ${connectionGroup.type}.${connectionGroup.subtype}`);
                
                if (connectionGroup.connections && Array.isArray(connectionGroup.connections)) {
                    connectionGroup.connections.forEach((connection, connIndex) => {
                        const connectedEntity = connection.connectedEntity;
                        
                        if (connectedEntity) {
                            const connectedEntityKey = getEntityKey(connectedEntity);
                            console.log(`    🔗 Связь ${connIndex + 1}: ${connectedEntityKey}`, {
                                type: connectedEntity.type,
                                INN: connectedEntity.INN,
                                hasINN: !!(connectedEntity.INN || connectedEntity.phOrgINN || connectedEntity.fzINN || connectedEntity.conINN)
                            });
                            
                            // Если сущность НОВАЯ и имеет ИНН
                            if (connectedEntityKey && 
                                !allProcessedKeys.has(connectedEntityKey) && 
                                !nextLevelEntities.has(connectedEntityKey)) {
                                
                                // ПРОВЕРЯЕМ ИНН для поиска
                                const hasINN = (connectedEntity.INN && connectedEntity.INN.trim() !== '') ||
                                              (connectedEntity.phOrgINN && connectedEntity.phOrgINN.trim() !== '') ||
                                              (connectedEntity.fzINN && connectedEntity.fzINN.trim() !== '') ||
                                              (connectedEntity.conINN && connectedEntity.conINN.trim() !== '');
                                
                                if (hasINN) {
                                    // Нормализуем сущность перед добавлением
                                    const normalizedEntity = normalizeEntityForConnections(connectedEntity);
                                    nextLevelEntities.set(connectedEntityKey, normalizedEntity);
                                    allProcessedKeys.add(connectedEntityKey);
                                    newEntitiesFound++;
                                    console.log(`    ✅ ДОБАВЛЕНА для уровня ${currentLevel + 1}: ${connectedEntityKey} (ИНН: ${connectedEntity.INN})`);
                                } else {
                                    console.log(`    ❌ ПРОПУЩЕНА (нет ИНН): ${connectedEntityKey}`);
                                }
                            } else if (connectedEntityKey) {
                                console.log(`    ⏩ ПРОПУЩЕНА (уже обработана): ${connectedEntityKey}`);
                            }
                        }
                    });
                }
            });
        }
    });

    console.log(`\n📊 ИТОГИ ПОДГОТОВКИ УРОВНЯ ${currentLevel + 1}:`);
    console.log(`   Найдено новых сущностей: ${newEntitiesFound}`);
    console.log(`   Всего для следующего уровня: ${nextLevelEntities.size}`);
    
    return Array.from(nextLevelEntities.values());
}

// --- СУЩЕСТВУЮЩИЕ ФУНКЦИИ (без изменений) ---

function prepareSearchData(normalizedEntities) {
    const searchData = {};
    
    SEARCH_CONFIGS.forEach(config => {
        const filteredEntities = normalizedEntities.filter(config.filter);
        console.log(`🔧 ${config.name}: ${filteredEntities.length} сущностей`);
        searchData[config.name] = {
            entities: filteredEntities,
            config: config
        };
    });
    
    return searchData;
}

async function executeConnectionsSearch(searchData) {
    const results = {};
    
    for (const config of SEARCH_CONFIGS) {
        const data = searchData[config.name];
        if (data.entities.length > 0) {
            console.log(`🔍 Запуск ${config.name} поиска для ${data.entities.length} сущностей`);
            results[config.name] = await config.findFunction(data.entities);
            console.log(`✅ ${config.name} поиск завершен: ${results[config.name].size} результатов`);
        } else {
            results[config.name] = new Map();
            console.log(`⏩ ${config.name} поиск пропущен: нет сущностей`);
        }
    }
    
    return results;
}

function normalizeEntityForConnections(entity) {
    if (!entity) return entity;

    const fieldMappings = {
        'inn': 'INN',
        'ogrn': 'OGRN',
        'name_short': 'NameShort', 
        'name_full': 'NameFull',
        'email': 'eMail',
        'phone': 'PhoneNum',
        'address_ur': 'AddressUr',
        'address_ufakt': 'AddressUFakt',
        'ur_fiz': 'UrFiz',
        'f_ip': 'fIP',
        'fzINN': 'INN',
        'conINN': 'INN',
        'phOrgINN': 'orgINN',
        'cpMail': 'eMail',
        'fzMail': 'eMail',
        'Contact': 'contactEmail'
    };

    // Применяем маппинг полей
    Object.entries(fieldMappings).forEach(([oldField, newField]) => {
        if (entity[oldField] !== undefined && entity[newField] === undefined) {
            entity[newField] = entity[oldField];
        }
    });

    // ВАЖНО: Для Delta сущностей добавляем sourceTable
    if (entity.source === 'delta' && !entity.sourceTable) {
        entity.sourceTable = `delta_${entity.type}`;
        console.log(`🔧 Нормализация Delta: добавлен sourceTable ${entity.sourceTable} для ${entity.INN}`);
    }

    // Приводим типы
    if (entity.fIP !== undefined) entity.fIP = Boolean(entity.fIP);
    if (entity.UrFiz !== undefined) entity.UrFiz = Number(entity.UrFiz);

    return entity;
}

export {
    findConnections,
    SEARCH_CONFIGS
};