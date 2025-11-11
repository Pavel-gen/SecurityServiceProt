/* eslint-disable no-unused-vars */
// find_connections.js
import { findConnectionsByEmail } from "./connectionByEmail.js";
import { findConnectionsByINN } from "./connectionByINNService.js";
import { findConnectionsByPhone } from "./connectionByPhoneService.js";

async function findConnections(entities) {
    // --- НОРМАЛИЗАЦИЯ ВХОДНЫХ ДАННЫХ ---

    const normalizedEntities = entities.map(normalizeEntityForConnections);
    console.log("Нормализованные сущности для поиска связей:", normalizedEntities);

    // --- ИНТЕГРАЦИЯ СВЯЗЕЙ ЧЕРЕЗ МОДЕЛЬ ---
    console.log(`Найдено ${normalizedEntities.length} нормализованных сущностей для поиска связей.`);
    const connectionsMap = new Map(); // Для хранения всех связей

    // --- НОВЫЙ БЛОК: Поиск связей по email ---
    const entitiesWithMailAndKey = normalizedEntities.filter(entity => {
         const entityKey = entity.UNID || entity.fzUID || entity.cpUID || entity.PersonUNID;
         return entityKey && entity.eMail && entity.eMail.trim() !== '';
    });
    console.log(`Найдено ${entitiesWithMailAndKey.length} сущностей с ключом и email для поиска связей.`);
    // const emailConnectionsMap = await findConnectionsByEmail(entitiesWithMailAndKey);

    // --- НОВЫЙ БЛОК: Поиск связей по телефону ---
    const entitiesWithPhoneAndKey = normalizedEntities.filter(entity => {
         const entityKey = entity.UNID || entity.fzUID || entity.cpUID || entity.PersonUNID;
         const hasPhone = entity.PhoneNum || entity.fzPhoneM || entity.cpPhoneMob || entity.cpPhoneWork || entity.Phone;
         return entityKey && hasPhone;
    });
    console.log(`Найдено ${entitiesWithPhoneAndKey.length} сущностей с ключом и телефоном для поиска связей.`);
    const phoneConnectionsMap = await findConnectionsByPhone(entitiesWithPhoneAndKey);

    // --- НОВЫЙ БЛОК: Поиск связей по ИНН ---
    const entitiesWithINN = normalizedEntities.filter(entity => {
         const entityKey = entity.UNID || entity.fzUID || entity.cpUID || entity.PersonUNID; // Добавляем PersonUNID
         // Используем нормализованное поле INN
         return entityKey && entity.INN && entity.INN.trim() !== '';
    });
    console.log(`Найдено ${entitiesWithINN.length} сущностей с ключом и INN для поиска связей.`);
    console.log("Сущности с ИНН:", entitiesWithINN); // Лог для проверки
    const innConnectionsMap = await findConnectionsByINN(entitiesWithINN);

    // --- СОБИРАЕМ СВЯЗИ В ОДИН ОБЪЕКТ ---
    const allResultsWithConnections = normalizedEntities.map(item => {
        const entityKey = item.UNID || item.fzUID || item.cpUID || item.PersonUNID;
        let entityConnections = [];

        // Добавляем связи по email
        if (entityKey && entitiesWithMailAndKey.some(e => (e.UNID || e.fzUID || e.cpUID || e.PersonUNID) === entityKey)) {
            // Реализация для email, если нужна
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
                    contact: innGroupKey, // ИНН
                    type: 'inn',
                    subtype: 'inn_match',
                    connections: connections
                });
            }
        }

        // Возвращаем копию элемента с добавленным полем connections
        return {
            ...item,
            connections: entityConnections,
            connectionsCount: entityConnections.length // Добавляем счётчик связей
        };
    });

    // --- НОВЫЙ БЛОК: Объединение связей для юридических лиц из Delta с связями, найденными для их prevwork ---
    // Проходим по всем результатам
    for (const resultItem of allResultsWithConnections) {
        // Проверяем, является ли сущность юридическим лицом (например, по типу или наличию NameFull/NameShort и отсутствию PersonUNID как основного ключа)
        // Также проверяем, есть ли у неё INN
        if (resultItem.type === 'juridical' && resultItem.INN) { // Уточните условие, если тип может быть другим или не всегда доступен
             // Ищем в connectionsMap все связи, которые были найдены для PersonUNID, совпадающих с INN этой юр.лица
             // Для этого нужно найти все сущности prevwork с тем же INN
             const prevWorkEntitiesForThisINN = normalizedEntities.filter(e => e.type === 'prevwork' && e.INN === resultItem.INN && e.PersonUNID);
             // Извлекаем их PersonUNID
             const personUNIDsOfPrevWork = prevWorkEntitiesForThisINN.map(e => e.PersonUNID);

             // Теперь ищем в connectionsMap (которая была сформирована findConnectionsByINN) связи для этих PersonUNID
             for (const personUNID of personUNIDsOfPrevWork) {
                 if (innConnectionsMap.has(personUNID)) {
                     const prevWorkConnections = innConnectionsMap.get(personUNID) || {};
                     // Добавляем все группы связей из prevWork к юр.лицу
                     for (const [prevWorkINNKey, connections] of Object.entries(prevWorkConnections)) {
                         // Создаём или находим группу связей по ИНН для юрлица
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
                         // Добавляем связи из prevWork к группе юрлица
                         juridicalINNConnectionGroup.connections.push(...connections);
                     }
                 }
             }
        }
    }

    // Пересчитываем connectionsCount после добавления связей
    allResultsWithConnections.forEach(item => {
        item.connectionsCount = item.connections.length;
    });

    // --- НОВЫЙ БЛОК: Поиск связей для ИНН, найденных в результатах связей (Второй уровень) ---
    // (Остальная логика второго уровня, если нужна, но может потребовать адаптации под новую структуру)

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