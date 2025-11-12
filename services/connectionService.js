/* eslint-disable no-unused-vars */
// find_connections.js
import { findConnectionsByEmail } from "./connectionByEmail.js";
import { findConnectionsByINN } from "./connectionByINNService.js";
import { findConnectionsByPhone } from "./connectionByPhoneService.js";
import { getEntityKey } from '../utils/helper.js';


async function findConnections(entities) {
    console.log("Запуск findConnections");

    // --- НОРМАЛИЗАЦИЯ ВХОДНЫХ ДАННЫХ ---
    const normalizedEntities = entities.map(normalizeEntityForConnections);
    console.log(`Найдено ${normalizedEntities.length} нормализованных сущностей для поиска связей.`);

    // --- СОБИРАЕМ ЦЕЛЕВЫЕ СУЩНОСТИ ДЛЯ РАЗНЫХ ТИПОВ ПОИСКА ---
    const entitiesWithKeyAndEmail = normalizedEntities.filter(entity => {
         return getEntityKey(entity) && entity.eMail && entity.eMail.trim() !== '';
    });
    console.log(`Найдено ${entitiesWithKeyAndEmail.length} сущностей с ключом и email для поиска связей.`);

    const entitiesWithKeyAndPhone = normalizedEntities.filter(entity => {
         const hasPhone = entity.PhoneNum || entity.fzPhoneM || entity.cpPhoneMob || entity.cpPhoneWork || entity.Phone;
         return getEntityKey(entity) && hasPhone;
    });
    console.log(`Найдено ${entitiesWithKeyAndPhone.length} сущностей с ключом и телефоном для поиска связей.`);

    const entitiesWithKeyAndINN = normalizedEntities.filter(entity => {
         return getEntityKey(entity) && entity.INN && entity.INN.trim() !== '';
    });
    console.log(`Найдено ${entitiesWithKeyAndINN.length} сущностей с ключом и INN для поиска связей.`);

    // --- ВЫПОЛНЯЕМ ПОИСК СВЯЗЕЙ ПАРАЛЛЕЛЬНО ---
    const [emailConnectionsMap, phoneConnectionsMap, innConnectionsMap] = await Promise.all([
        findConnectionsByEmail(entitiesWithKeyAndEmail),
        findConnectionsByPhone(entitiesWithKeyAndPhone), // Предполагается, что findConnectionsByPhone уже обновлена
        findConnectionsByINN(entitiesWithKeyAndINN)
    ]);

    // --- СОБИРАЕМ СВЯЗИ В ОДИН ОБЪЕКТ ---
    const allResultsWithConnections = normalizedEntities.map(item => {
        const entityKey = getEntityKey(item);
        let entityConnections = [];

        // Добавляем связи по email
        if (entityKey && emailConnectionsMap.has(entityKey)) {
            const emailConnections = emailConnectionsMap.get(entityKey) || {};
            for (const [emailGroupKey, connections] of Object.entries(emailConnections)) {
                entityConnections.push({
                    contact: emailGroupKey,
                    type: 'contact',
                    subtype: 'email',
                    connections: connections
                });
            }
        }

        // Добавляем связи по телефону
        if (entityKey && phoneConnectionsMap.has(entityKey)) {
            const phoneConnections = phoneConnectionsMap.get(entityKey) || {};
            for (const [phoneGroupKey, connections] of Object.entries(phoneConnections)) {
                entityConnections.push({
                    contact: phoneGroupKey,
                    type: 'contact',
                    subtype: 'phone',
                    connections: connections
                });
            }
        }

        // Добавляем связи по ИНН
        if (entityKey && innConnectionsMap.has(entityKey)) {
            const innConnections = innConnectionsMap.get(entityKey) || {};
            for (const [innGroupKey, connections] of Object.entries(innConnections)) {
                entityConnections.push({
                    contact: innGroupKey,
                    type: 'inn',
                    subtype: 'inn_match',
                    connections: connections
                });
            }
        }

        return {
            ...item,
            connections: entityConnections,
            connectionsCount: entityConnections.length
        };
    });

    // --- ОБЪЕДИНЕНИЕ СВЯЗЕЙ ДЛЯ ЮР.ЛИЦ ИЗ DELTA С ИХ PREVWORK ---
    // (Та же логика, что и раньше, но использует getEntityKey)
    for (const resultItem of allResultsWithConnections) {
        if (resultItem.type === 'juridical' && resultItem.INN) {
             const prevWorkEntitiesForThisINN = normalizedEntities.filter(e => e.type === 'prevwork' && e.INN === resultItem.INN && e.PersonUNID);
             const personUNIDsOfPrevWork = prevWorkEntitiesForThisINN.map(e => e.PersonUNID);

             for (const personUNID of personUNIDsOfPrevWork) {
                 const prevWorkEntity = normalizedEntities.find(e => e.type === 'prevwork' && e.INN === resultItem.INN && e.PersonUNID === personUNID);
                 if (prevWorkEntity) {
                     const prevWorkEntityKey = getEntityKey(prevWorkEntity);
                     if (prevWorkEntityKey && innConnectionsMap.has(prevWorkEntityKey)) {
                         const prevWorkConnections = innConnectionsMap.get(prevWorkEntityKey) || {};
                         for (const [prevWorkINNKey, connections] of Object.entries(prevWorkConnections)) {
                             let juridicalINNConnectionGroup = resultItem.connections.find(conn => conn.type === 'inn' && conn.subtype === 'inn_match' && conn.contact === prevWorkINNKey);
                             if (!juridicalINNConnectionGroup) {
                                 juridicalINNConnectionGroup = {
                                     contact: prevWorkINNKey,
                                     type: 'inn',
                                     subtype: 'inn_match',
                                     connections: []
                                 };
                                 resultItem.connections.push(juridicalINNConnectionGroup);
                             }
                             juridicalINNConnectionGroup.connections.push(...connections);
                         }
                     }
                 }
             }
        }
    }

    // Пересчитываем connectionsCount после добавления связей из prevWork
    allResultsWithConnections.forEach(item => {
        item.connectionsCount = item.connections.length;
    });

    // --- ПОИСК СВЯЗЕЙ ВТОРОГО УРОВНЯ (опционально) ---
    // (Остальная логика второго уровня, если нужна)

    console.log(`Возвращаем ${allResultsWithConnections.length} сущностей с найденными связями.`);
    return allResultsWithConnections;
}

// --- ВСПОМОГАТЕЛЬНАЯ ФУНКЦИЯ: Нормализация сущности для поиска связей ---
// Применяет те же правила, что и в findLocalData
function normalizeEntityForConnections(entity) {
    if (!entity) return entity;
    // Нормализуем ИНН
    if (entity && entity.inn && !entity.INN) {
        entity.INN = entity.inn;
    }
    // Нормализуем OGRN
    if (entity && entity.ogrn && !entity.OGRN) {
        entity.OGRN = entity.ogrn;
    }
    // Нормализуем NameShort
    if (entity && entity.name_short && !entity.NameShort) {
        entity.NameShort = entity.name_short;
    }
    // Нормализуем NameFull
    if (entity && entity.name_full && !entity.NameFull) {
        entity.NameFull = entity.name_full;
    }
    // Нормализуем eMail
    if (entity && entity.email && !entity.eMail) {
        entity.eMail = entity.email;
    }
    // Нормализуем PhoneNum
    if (entity && entity.phone && !entity.PhoneNum) {
        entity.PhoneNum = entity.phone;
    }
    // Нормализуем AddressUr
    if (entity && entity.address_ur && !entity.AddressUr) {
        entity.AddressUr = entity.address_ur;
    }
    // Нормализуем AddressUFakt
    if (entity && entity.address_ufakt && !entity.AddressUFakt) {
        entity.AddressUFakt = entity.address_ufakt;
    }
    // Нормализуем UrFiz
    if (entity && entity.ur_fiz && !entity.UrFiz) {
        entity.UrFiz = entity.ur_fiz;
    }
    // Нормализуем fIP
    if (entity && entity.f_ip !== undefined && entity.fIP === undefined) {
        entity.fIP = entity.f_ip;
    }

    // Убедимся, что fIP - boolean, если оно есть
    if (entity && entity.fIP !== undefined) {
        entity.fIP = Boolean(entity.fIP);
    }
    // Убедимся, что UrFiz - число, если оно есть
    if (entity && entity.UrFiz !== undefined) {
        entity.UrFiz = Number(entity.UrFiz);
    }

    return entity;
}

// ... (остальной код find_connections.js остается без изменений) ...

export {
    findConnections,
};