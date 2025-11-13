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
                
                // Проверяем ВСЕ возможные поля с email
                const hasEmail = (entity.eMail && entity.eMail.trim() !== '') ||
                                (entity.cpMail && entity.cpMail.trim() !== '') ||
                                (entity.fzMail && entity.fzMail.trim() !== '') ||
                                (entity.Contact && entity.Contact.includes('@')); // для CF_Contacts
                
                return hasEmail;
            },
            findFunction: findConnectionsByEmail,
            type: 'contact',
            subtype: 'email'
        }
    // {
    //     name: 'inn',
    //     filter: entity => {
    //         const entityKey = getEntityKey(entity);
    //         if (!entityKey) return false;
            
    //         const hasAnyINN = (entity.INN && entity.INN.trim() !== '') ||
    //                          (entity.phOrgINN && entity.phOrgINN.trim() !== '') ||
    //                          (entity.fzINN && entity.fzINN.trim() !== '') ||
    //                          (entity.conINN && entity.conINN.trim() !== '');
            
    //         return hasAnyINN;
    //     },
    //     findFunction: findConnectionsByINN,
    //     type: 'inn',
    //     subtype: 'inn_match'
    // }
];

async function findConnections(entities) {
    console.log("Запуск findConnections");

    // --- НОРМАЛИЗАЦИЯ ВХОДНЫХ ДАННЫХ ---
    const normalizedEntities = entities.map(normalizeEntityForConnections);
    console.log(`Найдено ${normalizedEntities.length} нормализованных сущностей для поиска связей.`);

    // --- ПОДГОТОВКА ДАННЫХ ДЛЯ ПОИСКА ---
    const searchData = prepareSearchData(normalizedEntities);
    
    // --- ВЫПОЛНЯЕМ ПОИСК СВЯЗЕЙ ПАРАЛЛЕЛЬНО ---
    const connectionsResults = await executeConnectionsSearch(searchData);
    
    // --- СОБИРАЕМ РЕЗУЛЬТАТЫ ---
    const allResultsWithConnections = buildResultsWithConnections(
        normalizedEntities, 
        searchData, 
        connectionsResults
    );

    // --- ОБРАБОТКА ДОПОЛНИТЕЛЬНЫХ СВЯЗЕЙ ---
    processAdditionalConnections(allResultsWithConnections, normalizedEntities, connectionsResults.inn);

    console.log(`Возвращаем ${allResultsWithConnections.length} сущностей с найденными связями.`);
    return allResultsWithConnections;
}

// --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---

function prepareSearchData(normalizedEntities) {
    const searchData = {};
    
    SEARCH_CONFIGS.forEach(config => {
        const filteredEntities = normalizedEntities.filter(config.filter);
        console.log(`Найдено ${filteredEntities.length} сущностей для поиска по ${config.name}`);
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
            results[config.name] = await config.findFunction(data.entities);
        } else {
            results[config.name] = new Map();
        }
    }
    
    return results;
}

function buildResultsWithConnections(normalizedEntities, searchData, connectionsResults) {
    // console.log('=== DEBUG buildResultsWithConnections ===');
    // console.log('Входные сущности:', normalizedEntities.length);
    // console.log('Connections results:', {
    //     email: connectionsResults.email?.size || 0,
    //     phone: connectionsResults.phone?.size || 0, 
    //     inn: connectionsResults.inn?.size || 0
    // });
    
    const results = normalizedEntities.map(item => {
        const entityKey = getEntityKey(item);
        let entityConnections = [];
        
        SEARCH_CONFIGS.forEach(config => {
            const connectionsMap = connectionsResults[config.name];
            if (entityKey && connectionsMap && connectionsMap.has(entityKey)) {
                const connections = connectionsMap.get(entityKey) || {};
                // console.log(`Найдены связи для ${entityKey}:`, Object.keys(connections));
                
                for (const [contactKey, connectionList] of Object.entries(connections)) {
                    entityConnections.push({
                        contact: contactKey,
                        type: config.type,
                        subtype: config.subtype,
                        connections: connectionList
                    });
                }
            }
        });
        
        return {
            ...item,
            connections: entityConnections,
            connectionsCount: entityConnections.length
        };
    });
    
    // console.log('Финальные результаты с связями:');
    // results.forEach(result => {
    //     if (result.connections.length > 0) {
    //         console.log(`  ${getEntityKey(result)}: ${result.connections.length} связей`);
    //     }
    // });
    
    return results;
}

function processAdditionalConnections(results, normalizedEntities, innConnectionsMap) {
    // Обработка связей через предыдущие места работы
    results.forEach(resultItem => {
        if (resultItem.type === 'juridical' && resultItem.INN) {
            const prevWorkEntities = normalizedEntities.filter(
                e => e.type === 'prevwork' && e.INN === resultItem.INN && e.PersonUNID
            );

            prevWorkEntities.forEach(prevWorkEntity => {
                const prevWorkEntityKey = getEntityKey(prevWorkEntity);
                if (prevWorkEntityKey && innConnectionsMap.has(prevWorkEntityKey)) {
                    addPrevWorkConnections(resultItem, prevWorkEntityKey, innConnectionsMap);
                }
            });
        }
    });

    // Пересчитываем количество связей
    results.forEach(item => {
        item.connectionsCount = item.connections.length;
    });
}

function addPrevWorkConnections(resultItem, prevWorkEntityKey, innConnectionsMap) {
    const prevWorkConnections = innConnectionsMap.get(prevWorkEntityKey) || {};
    
    for (const [prevWorkINNKey, connections] of Object.entries(prevWorkConnections)) {
        let connectionGroup = resultItem.connections.find(
            conn => conn.type === 'inn' && conn.subtype === 'inn_match' && conn.contact === prevWorkINNKey
        );
        
        if (!connectionGroup) {
            connectionGroup = {
                contact: prevWorkINNKey,
                type: 'inn',
                subtype: 'inn_match',
                connections: []
            };
            resultItem.connections.push(connectionGroup);
        }
        
        connectionGroup.connections.push(...connections);
    }
}

// --- НОРМАЛИЗАЦИЯ СУЩНОСТЕЙ ---
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
        // ДОБАВЛЯЕМ ПОЛЯ ДЛЯ EMAIL:
        'cpMail': 'eMail',           // для контактных лиц
        'fzMail': 'eMail',           // для сотрудников
        'Contact': 'contactEmail'    // для контактов (отдельное поле чтобы не перезаписать)
    };

    // Применяем маппинг полей
    Object.entries(fieldMappings).forEach(([oldField, newField]) => {
        if (entity[oldField] !== undefined && entity[newField] === undefined) {
            entity[newField] = entity[oldField];
        }
    });

    // Приводим типы
    if (entity.fIP !== undefined) entity.fIP = Boolean(entity.fIP);
    if (entity.UrFiz !== undefined) entity.UrFiz = Number(entity.UrFiz);

    return entity;
}


export {
    findConnections,
    SEARCH_CONFIGS // Экспортируем для тестирования
};