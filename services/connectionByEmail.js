import sql from 'mssql'
import { determineEntityType } from '../utils/helper.js';
import { getEntityKey } from '../utils/helper.js'; // Импортируем getEntityKey
import { buildEmailQuery } from '../queries/email.queries.js';

// --- ВСПОМОГАТЕЛЬНАЯ ФУНКЦИЯ: Построение SQL-запроса для ПОИСКА по email (прямой и косвенный через CF_Contacts) ---
// --- ВСПОМОГАТЕЛЬНАЯ ФУНКЦИЯ: Построение SQL-запроса для ПОИСКА по email (прямой и косвенный через CF_Contacts) ---


// --- ОСНОВНАЯ ФУНКЦИЯ: Поиск связей по email ---
async function findConnectionsByEmail(targetEntities) {
    console.log("Запуск findConnectionsByEmail");

    // Собираем уникальные email из целевых сущностей
    const targetEmails = new Set();
    const entitiesByKey = new Map();

    targetEntities.forEach(entity => {
        const entityKey = getEntityKey(entity);
        if (entityKey && entity.eMail && entity.eMail.trim() !== '') {
            const emails = entity.eMail.toLowerCase().split(';').map(email => email.trim()).filter(email => email);
            emails.forEach(email => targetEmails.add(email));
            entitiesByKey.set(entityKey, entity);
            console.log(`Добавлена сущность с ключом ${entityKey} и email ${entity.eMail} в entitiesByKey`);
        } else {
            console.log(`Сущность не имеет ключа или eMail, пропускаем:`, entity);
        }
    });

    const emailArray = Array.from(targetEmails).filter(email => email);
    console.log("Целевые email для поиска связей:", emailArray);
    console.log("entitiesByKey размер:", entitiesByKey.size);

    if (emailArray.length === 0) {
        console.log("Нет email для поиска связей.");
        const connectionsMap = new Map();
        entitiesByKey.forEach((entity, entityKey) => {
            if (!connectionsMap.has(entityKey)) {
                connectionsMap.set(entityKey, {});
            }
        });
        return connectionsMap;
    }

    const connectionsMap = new Map();
    entitiesByKey.forEach((entity, entityKey) => {
        if (!connectionsMap.has(entityKey)) {
            connectionsMap.set(entityKey, {});
        }
    });

    // --- Выполнение поиска по email ---
    const fullEmailQuery = buildEmailQuery(emailArray);
    console.log("Выполняемый SQL для email:", fullEmailQuery);

    const emailRequest = new sql.Request();
    emailArray.forEach((email, index) => {
        emailRequest.input(`email${index}`, sql.VarChar, email.toLowerCase());
    });

    try {
        const emailResult = await emailRequest.query(fullEmailQuery);
        console.log("Количество результатов поиска по email:", emailResult.recordset.length);

        emailResult.recordset.forEach(row => {
            let connectedType = 'unknown';
            // Для 'contact' сущности, NameShort может быть неуместным, лучше использовать email
            let connectedName = (row.sourceTable === 'contact') ? row.contactEmail : (row.contactNameShort || row.contactNameFull || row.contactEmail || 'N/A');
            let connectedEntityKey = row.entityKey;
            let connectionStatus = 'unknown_status';
            let baseName = row.baseName || null;
            const prevWorkCaption = row.prevWorkCaption || null;

            if (!connectedEntityKey) {
                console.log(`Предупреждение: Не удалось определить ключ для найденной сущности из ${row.sourceTable}. Пропускаем.`, row);
                return;
            }

            if (row.sourceTable === 'person_from_prevwork_via_contact') { // <<< ОБНОВЛЁННЫЙ СЛУЧАЙ >>>
                connectedType = 'physical';
                connectionStatus = 'person_match_via_contact_from_prevwork';
                // connectionStatus = 'person_match_via_contact'; // Старый статус, если не хотите новый
            } else if (row.sourceTable === 'contragent') {
                connectedType = determineEntityType(row.UrFiz, row.fIP);
                connectionStatus = 'organization_match';
            } else if (row.sourceTable === 'employee') {
                connectedType = 'physical';
                connectionStatus = 'employee_match';
            } else if (row.sourceTable === 'contperson') {
                connectedType = 'physical';
                connectionStatus = 'contact_person_match';
            } else if (row.sourceTable === 'prevwork') {
                connectedType = 'legal';
                connectionStatus = 'prevwork_match';
            } else if (row.sourceTable === 'person_via_contact') {
                connectedType = 'physical';
                connectionStatus = 'person_match_via_contact';
            } else if (row.sourceTable === 'employee_via_contact') {
                connectedType = 'physical';
                connectionStatus = 'employee_match_via_contact';
            } else if (row.sourceTable === 'contperson_via_contact') {
                connectedType = 'physical';
                connectionStatus = 'contact_person_match_via_contact';
            } else if (row.sourceTable === 'prevwork_via_contact') {
                connectedType = 'legal';
                connectionStatus = 'prevwork_match_via_contact';
            } else if (row.sourceTable === 'contact') {
                 connectedType = 'contact'; // Новый тип для самой сущности контакта
                 connectionStatus = 'contact_found'; // Статус - найден в таблице контактов
                 // connectedName уже установлен как email выше
            } else if (row.sourceTable === 'person_from_prevwork_email') {
                 connectedType = 'physical'; // Это человек из CF_Persons
                 connectionStatus = 'person_match_from_prevwork_email'; // Выберите подходящий статус
            }
// .

            const foundEmail = row.contactEmail.toLowerCase();

            if (!emailArray.includes(foundEmail)) {
                 console.log(`Найденная сущность ${connectedEntityKey} имеет email '${foundEmail}', но оно не является целевым. Пропускаем.`);
                 return;
            }

            entitiesByKey.forEach((targetEntity, targetEntityKey) => {
                const targetEmailsList = (targetEntity.eMail || '').toLowerCase().split(';').map(email => email.trim()).filter(email => email);
                if (targetEmailsList.includes(foundEmail)) {
                    // Проверяем, что найденная сущность не является целевой (по ключу)
                    if (connectedEntityKey !== targetEntityKey) {
                        // Группируем по найденному email
                        if (!connectionsMap.get(targetEntityKey)[foundEmail]) {
                            connectionsMap.get(targetEntityKey)[foundEmail] = [];
                        }
                        connectionsMap.get(targetEntityKey)[foundEmail].push({
                            connectedEntity: {
                                INN: row.contactINN,
                                NameShort: connectedName,
                                NameFull: row.contactNameFull,
                                type: connectedType,
                                sourceTable: row.sourceTable,
                                source: 'local',
                                baseName: baseName,
                                PersonUNID: row.PersonUNID,
                                prevWorkCaption: prevWorkCaption // Добавляем в connectedEntity

                            },
                            connectionType: (row.relatedPersonUNID ? 'person_unid_via_email_match' : 'email_match'), // Уточняем тип связи
                            connectionStatus: connectionStatus,
                            connectionDetails: `Совпадение по email: ${foundEmail}, найдено в таблице ${row.sourceTable} (косвенно через PersonUNID: ${!!row.relatedPersonUNID}), статус: ${connectionStatus}`
                        });
                    }
                }
            });
        });

    } catch (err) {
        console.error('Ошибка при поиске связей по email:', err);
        throw err; // Прерываем выполнение, если основной поиск не удался
    }

    console.log(`Итоговый размер connectionsMap (email): ${connectionsMap.size}`);
    return connectionsMap;
}

export { findConnectionsByEmail }; // Экспортируем функцию и вспомогательную для тестов, если нужно