import { cleanPhone, determineEntityType, determineEntityTypeSpec } from "../utils/helper.js";
import sql from 'mssql'
import { buildPhoneQuery } from '../queries/phone.queries.js'; // Импортируем новую функцию
import { getEntityKey } from '../utils/helper.js'; // Импортируем getEntityKey

function getEntityPhones(entity) {
    const phoneFields = [
        'PhoneNum', 'fzPhone', 'fzPhoneM', 'cpPhoneMob', 'cpPhoneMobS', 
        'cpPhoneWork', 'Phone', 'Contact'
    ];
    
    let phones = [];
    phoneFields.forEach(field => {
        if (entity[field]) {
            phones.push(...entity[field].split(';').map(phone => phone.trim()).filter(phone => phone));
        }
    });
    return phones;
}

// --- ОСНОВНАЯ ФУНКЦИЯ: Поиск связей по телефону ---
async function findConnectionsByPhone(targetEntities) {
    console.log("Запуск findConnectionsByPhone");

    const targetPhones = new Set();
    const entitiesByKey = new Map();

    targetEntities.forEach(entity => {
        const entityKey = getEntityKey(entity);
        if (entityKey) {
            const targetPhonesList = getEntityPhones(entity);
            const targetCleanedPhones = targetPhonesList.map(cleanPhone).filter(phone => phone);

            targetCleanedPhones.forEach(phone => {
                if (phone) targetPhones.add(phone);
            });
            entitiesByKey.set(entityKey, entity);
        }
    });

    const phoneArray = Array.from(targetPhones).filter(phone => phone);

    console.log("Целевые телефоны для поиска связей:", phoneArray);

    const connectionsMap = new Map();
    entitiesByKey.forEach((entity, entityKey) => {
        if (!connectionsMap.has(entityKey)) {
            connectionsMap.set(entityKey, {});
        }
    });

    // Добавим Map для новых сущностей
    const newFoundEntities = new Map();

    if (phoneArray.length > 0) {
        try {
            const fullPhoneQuery = buildPhoneQuery(phoneArray);
            const phoneRequest = new sql.Request();
            phoneArray.forEach((phone, index) => {
                phoneRequest.input(`phone${index}`, sql.VarChar, phone);
            });

            const phoneResult = await phoneRequest.query(fullPhoneQuery);
            console.log("Количество результатов поиска по телефону:", phoneResult.recordset.length);

            phoneResult.recordset.forEach(row => {
                let connectedType = determineEntityType(row);
                let connectedName = row.contactNameShort || row.contactNameFull || 'N/A';
                let connectedEntityKey = row.entityKey;
                let connectionStatus = 'unknown_status';
                let baseName = row.baseName || null;               
                
                if (!connectedEntityKey) return;

                const foundPhone = row.contactPhone;
                const foundCleanedPhone = cleanPhone(foundPhone);

                if (!phoneArray.includes(foundCleanedPhone)) return;

                entitiesByKey.forEach((targetEntity, targetEntityKey) => {
                    const targetPhonesList = getEntityPhones(targetEntity);
                    const targetCleanedPhones = targetPhonesList.map(cleanPhone).filter(phone => phone);

                    if (targetCleanedPhones.includes(foundCleanedPhone)) {
                        if (connectedEntityKey !== targetEntityKey) {
                            if (!connectionsMap.get(targetEntityKey)[foundCleanedPhone]) {
                                connectionsMap.get(targetEntityKey)[foundCleanedPhone] = [];
                            }
                            const existingConnection = connectionsMap.get(targetEntityKey)[foundCleanedPhone].find(conn => {
                                return (conn.connectedEntity.INN === row.contactINN && conn.connectedEntity.NameShort === connectedName && conn.connectedEntity.type === connectedType);
                            });
                            if (!existingConnection) {
                                connectionsMap.get(targetEntityKey)[foundCleanedPhone].push({
                                    connectedEntity: {
                                        INN: row.contactINN,
                                        NameShort: connectedName,
                                        NameFull: row.contactNameFull,
                                        type: connectedType,
                                        sourceTable: row.sourceTable,
                                        source: 'local',
                                        baseName: baseName,
                                        PersonUNID: row.PersonUNID,
                                        prevWorkCaption: row.prevWorkCaption,
                                        UrFiz: row.UrFiz,
                                        fIP: row.fIP
                                    },
                                    connectionType: 'phone_match',
                                    connectionStatus: connectionStatus,
                                    connectionDetails: `Совпадение по телефону: ${foundPhone}, найдено в таблице ${row.sourceTable}, статус: ${connectionStatus}`
                                });
                            }
                        }
                    }
                });
            })    
        } catch (err) {
            console.error('Ошибка при поиске связей по телефону:', err);
            console.error('Stack:', err.stack);
        }
    } else {
        console.log("Нет телефонов для поиска связей (phoneArray пуст).");
    }

    console.log(`Итоговый размер connectionsMap (телефон): ${connectionsMap.size}`);
    
    // Возвращаем объект с обоими результатами
    return connectionsMap;
}


export {findConnectionsByPhone}