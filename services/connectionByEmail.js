import sql from 'mssql'
import { determineEntityType } from '../utils/helper.js';

async function findConnectionsByEmail(targetEntities) {
    // Собираем уникальные email из целевых сущностей
    const targetEmails = new Set();
    const entitiesByKey = new Map(); // Используем ключ (UNID или fzUID)

    targetEntities.forEach(entity => {
        const entityKey = entity.UNID || entity.fzUID; // Новый ключ: UNID (для CI_Contragent) или fzUID (для CI_Employees)
        if (entityKey && entity.eMail && entity.eMail.trim() !== '') { // Проверяем наличие ключа
            const emails = entity.eMail.toLowerCase().split(';').map(email => email.trim()).filter(email => email);
            console.log(`Обработка целевой сущности: ${entityKey}, eMail: ${entity.eMail}, Name: ${entity.NameShort || entity.fzFIO}`); // Лог
            emails.forEach(email => targetEmails.add(email));
            entitiesByKey.set(entityKey, entity); // Сохраняем по новому ключу
        }
    });

    const emailArray = Array.from(targetEmails).filter(email => email);

    console.log("Целевые email для поиска связей:", emailArray); // Лог

    const connectionsMap = new Map();
    // Инициализируем connectionsMap для всех целевых сущностей по их ключу
    entitiesByKey.forEach((entity, entityKey) => {
        if (!connectionsMap.has(entityKey)) {
            connectionsMap.set(entityKey, {});
            console.log(`Инициализирована карта связей для ключа: ${entityKey}`); // Лог
        }
    });

    if (emailArray.length > 0) {
        // Подготовим условия для WHERE - ищем точное совпадение email
        let emailORCondition = '';
        emailArray.forEach((email, index) => {
            emailORCondition += `(LOWER(ci.eMail) LIKE @email${index})`;
            if (index < emailArray.length - 1) emailORCondition += ' OR ';
        });

        // Запрос 1: Совпадения в CI_Contragent_test.eMail
        const contragentEmailQuery = `
            SELECT
                ci.UNID as contactUNID, -- Получаем UNID вместо INN
                ci.INN as contactINN, -- Оставим INN для отображения
                ci.NameShort as contactNameShort,
                ci.NameFull as contactNameFull,
                ci.eMail as contactEmail,
                ci.UrFiz,
                ci.fIP,
                NULL as fzUID -- У юрлиц/ип/физлиц из CI_Contragent fzUID нет
            FROM CI_Contragent_test ci
            WHERE ${emailORCondition}
        `;

        // Запрос 2: Совпадения в CI_Employees_test.fzMail
        const employeeEmailQuery = `
            SELECT
                NULL as contactUNID, -- У сотрудника нет UNID из CI_Contragent
                NULL as contactINN, -- ИНН организации не возвращаем, если не делаем JOIN или он не нужен
                ce.fzFIO as contactNameShort,
                ce.fzFIO as contactNameFull,
                ce.fzMail as contactEmail,
                2 as UrFiz, -- Предполагаем, что сотрудник - физлицо
                0 as fIP,   -- Предполагаем, что сотрудник - не ИП (в рамках нашей логики отображения)
                ce.fzUID as fzUID -- fzUID сотрудника
            FROM CI_Employees_test ce
            WHERE LOWER(ce.fzMail) IN (${emailArray.map((_, idx) => `@email${idx}`).join(', ')}) -- Точное совпадение
        `;

        const fullEmailQuery = `${contragentEmailQuery} UNION ALL ${employeeEmailQuery}`;

        console.log("Выполняемый SQL для email:", fullEmailQuery); // Лог

        const emailRequest = new sql.Request();
        emailArray.forEach((email, index) => {
            emailRequest.input(`email${index}`, sql.VarChar, email); // Передаем просто email для точного совпадения
        });

        try {
            const emailResult = await emailRequest.query(fullEmailQuery);
            console.log("Количество результатов поиска по email:", emailResult.recordset.length); // Лог
            // Выведем первые несколько результатов для проверки
            console.log("Примеры найденных сущностей:", emailResult.recordset.slice(0, 5)); // Лог

            emailResult.recordset.forEach(row => {
                const connectedType = determineEntityType(row.UrFiz, row.fIP);
                let connectedName = row.contactNameShort || row.contactNameFull || 'N/A';
                const foundEmail = row.contactEmail;

                const foundEmails = foundEmail.toLowerCase().split(';').map(email => email.trim()).filter(email => email);
                const matchingTargetEmails = foundEmails.filter(found_addr => emailArray.includes(found_addr));

                if (matchingTargetEmails.length === 0) {
                     console.log(`Найденная сущность ${row.contactUNID || row.fzUID} имеет email '${foundEmail}', но не содержит целевых emails. Пропускаем.`);
                     return;
                }

                // console.log(`Обработка найденной сущности: ${row.contactUNID || row.fzUID}, eMail: ${foundEmail}, Name: ${connectedName}`); // Лог

                // Находим целевые сущности, у которых есть этот email
                entitiesByKey.forEach((targetEntity, targetEntityKey) => {
                    const targetEmailsList = targetEntity.eMail.toLowerCase().split(';').map(email => email.trim()).filter(email => email);
                    const matchingTargetEmailsInEntity = targetEmailsList.filter(target_addr => emailArray.includes(target_addr));

                    const connectedEntityKey = row.contactUNID || row.fzUID;

                    // Проверяем, что найденная сущность не является целевой (по ключу)
                    if (matchingTargetEmailsInEntity.length > 0 && connectedEntityKey !== targetEntityKey) {
                        // console.log(`Совпадение найдено: целевая сущность ${targetEntityKey}, найденная сущность ${connectedEntityKey}`); // Лог
                        const intersection = matchingTargetEmails.filter(addr => targetEmailsList.includes(addr));
                        if (intersection.length > 0) {
                            intersection.forEach(intersectingEmail => {
                                if (!connectionsMap.get(targetEntityKey)[intersectingEmail]) {
                                    connectionsMap.get(targetEntityKey)[intersectingEmail] = [];
                                }
                                // Лог перед добавлением
                                // console.log(`Добавление связи: целевая ${targetEntityKey} -> найденная ${connectedEntityKey}, email ${intersectingEmail}`); // Лог
                                connectionsMap.get(targetEntityKey)[intersectingEmail].push({
                                    connectedEntity: {
                                        INN: row.contactINN, // ИНН связанной сущности (если есть)
                                        NameShort: connectedName,
                                        type: connectedType
                                    },
                                    connectionType: 'email_match',
                                    connectionDetails: `Совпадение по email: ${intersectingEmail}`
                                });
                            });
                        }
                    } else {
                        if (connectedEntityKey === targetEntityKey) {
                            // console.log(`Пропуск: целевая и найденная сущности совпадают по ключу ${targetEntityKey}`); // Лог
                        }
                    }
                });
            });
        } catch (err) {
            console.error('Ошибка при поиске связей по email:', err);
            throw err;
        }
    } else {
        console.log("Нет email для поиска связей.");
    }

    // Выведем итоговый размер connectionsMap и несколько примеров
    console.log(`Итоговый размер connectionsMap: ${connectionsMap.size}`);
    for (const [key, value] of connectionsMap.entries()) {
        console.log(`Ключ ${key}: количество email-групп связей: ${Object.keys(value).length}`);
        // Покажем количество связей в первой группе, если есть
        const firstEmailKey = Object.keys(value)[0];
        if (firstEmailKey) {
            console.log(`  - для email '${firstEmailKey}': ${value[firstEmailKey].length} связей`);
        }
    }

    return connectionsMap; // Возвращаем карту связей, сгруппированных по ключу (UNID или fzUID) и затем по email
}

export { findConnectionsByEmail }