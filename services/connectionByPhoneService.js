import { cleanPhone, determineEntityType } from "../utils/helper.js";
import sql from 'mssql'
import { buildPhoneQuery } from '../queries/phone.queries.js'; // Импортируем новую функцию
import { getEntityKey } from '../utils/helper.js'; // Импортируем getEntityKey

// --- ОСНОВНАЯ ФУНКЦИЯ: Поиск связей по телефону ---
async function findConnectionsByPhone(targetEntities) {
    console.log("Запуск findConnectionsByPhone");

    const targetPhones = new Set();
    const entitiesByKey = new Map();

    targetEntities.forEach(entity => {
        const entityKey = getEntityKey(entity);
        if (entityKey) {
            const phones = [];
            if (entity.PhoneNum) phones.push(...entity.PhoneNum.split(';').map(phone => phone.trim()).filter(phone => phone));
            if (entity.fzPhoneM) phones.push(...entity.fzPhoneM.split(';').map(phone => phone.trim()).filter(phone => phone));
            if (entity.cpPhoneMob) phones.push(...entity.cpPhoneMob.split(';').map(phone => phone.trim()).filter(phone => phone));
            if (entity.cpPhoneWork) phones.push(...entity.cpPhoneWork.split(';').map(phone => phone.trim()).filter(phone => phone));
            if (entity.Phone) phones.push(...entity.Phone.split(';').map(phone => phone.trim()).filter(phone => phone));

            phones.forEach(phone => {
                const cleanedPhone = cleanPhone(phone);
                if (cleanedPhone) targetPhones.add(cleanedPhone);
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

    if (phoneArray.length > 0) {
        // Обернём ВЕСЬ блок кода внутри if в try...catch
        try {
            console.log("Вызываем buildPhoneQuery...");
            const fullPhoneQuery = buildPhoneQuery(phoneArray);
            console.log("buildPhoneQuery выполнен успешно, длина запроса:", fullPhoneQuery.length);

            const phoneRequest = new sql.Request();
            phoneArray.forEach((phone, index) => {
                phoneRequest.input(`phone${index}`, sql.VarChar, phone);
            });
            console.log("Параметры для запроса подготовлены.");

            // console.log("Выполняем SQL запрос...", fullPhoneQuery);
            const phoneResult = await phoneRequest.query(fullPhoneQuery); // <<< ОШИБКА ЗДЕСЬ >>>
            console.log("Количество результатов поиска по телефону:", phoneResult.recordset.length); // <<< ЭТОТ ЛОГ НЕ ПОЯВИТСЯ >>>
            // console.log("Найденные сущности по телефону: ", phoneResult);

            // --- СОПОСТАВЛЕНИЕ НАЙДЕННЫХ СУЩНОСТЕЙ С ЦЕЛЕВЫМИ ---
            phoneResult.recordset.forEach(row => {
                // ... (ваша логика сопоставления) ...
                // (оставлю только ключевые части для краткости)
                let connectedType = 'unknown';
                let connectedName = row.contactNameShort || row.contactNameFull || 'N/A';
                let connectedEntityKey = row.entityKey;
                let connectionStatus = 'unknown_status';
                let baseName = row.baseName || null;                if (!connectedEntityKey) return;


                // ... (определение connectedType, connectedName, connectionStatus, baseName) ...

                const foundPhone = row.contactPhone;
                const foundCleanedPhone = cleanPhone(foundPhone);

                if (!phoneArray.includes(foundCleanedPhone)) return;

                entitiesByKey.forEach((targetEntity, targetEntityKey) => {
                    const targetPhonesList = [];
                    if (targetEntity.PhoneNum) targetPhonesList.push(...targetEntity.PhoneNum.split(';').map(p => p.trim()).filter(p => p));
                    if (targetEntity.fzPhoneM) targetPhonesList.push(...targetEntity.fzPhoneM.split(';').map(p => p.trim()).filter(p => p));
                    if (targetEntity.cpPhoneMob) targetPhonesList.push(...targetEntity.cpPhoneMob.split(';').map(p => p.trim()).filter(p => p));
                    if (targetEntity.cpPhoneWork) targetPhonesList.push(...targetEntity.cpPhoneWork.split(';').map(p => p.trim()).filter(p => p));
                    if (targetEntity.Phone) targetPhonesList.push(...targetEntity.Phone.split(';').map(p => p.trim()).filter(p => p));

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
                                        prevWorkCaption: row.prevWorkCaption
                                    },
                                    connectionType: 'phone_match',
                                    connectionStatus: connectionStatus,
                                    connectionDetails: `Совпадение по телефону: ${foundPhone}, найдено в таблице ${row.sourceTable}, статус: ${connectionStatus}`
                                });
                            }
                        }
                    }
                });
            });

        } catch (err) {
            // Этот catch должен сработать при ошибке в любом месте внутри try
            console.error('Ошибка при поиске связей по телефону:', err);
            console.error('Stack:', err.stack);
            // Важно: не бросаем ошибку дальше, если findConnections должен продолжить работу с другими типами связей
            // Но connectionsMap останется пустым или частично заполненным на момент ошибки
        }
    } else {
        console.log("Нет телефонов для поиска связей (phoneArray пуст).");
    }

    console.log(`Итоговый размер connectionsMap (телефон): ${connectionsMap.size}`);
    return connectionsMap; // Даже если был catch, возвращаем текущее состояние connectionsMap
}


export {findConnectionsByPhone}