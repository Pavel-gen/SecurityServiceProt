// inn.queries.js
import { normalizePhoneSQL } from '../utils/helper.js'; // Если нужна для телефонов в будущем

export function buildINNQuery(innArray) {
    if (innArray.length === 0) return 'SELECT 1 as dummy WHERE 1=0'; // Пустой запрос, если нет ИНН

    const innParams = innArray.map((inn, index) => `@inn${index}`);

    // --- Запросы для поиска по ИНН юридических лиц (и ИП) ---
    // Запрос 1: Совпадения в CI_Contragent.INN (поиск самого юр.лица/ИП по ИНН)
    const contragentINNQuery = `
        SELECT
            ci.UNID as contactUNID,
            ci.INN as contactINN, -- ИНН юр.лица
            ci.NameShort as contactNameShort,
            ci.NameFull as contactNameFull,
            ci.UrFiz,
            ci.fIP,
            NULL as fzUID,
            NULL as cpUID,
            NULL as PersonUNID,
            NULL as fzFIO,
            NULL as phFunction,
            NULL as phEventType,
            NULL as phDate,
            NULL as PersonLastName,
            NULL as PersonFirstName,
            NULL as PersonMiddleName,
            NULL as PersonSNILS,
            'CI_Contragent_test' as sourceTable, -- <<< Имя таблицы
            ci.UNID as entityKey, -- <<< Ключ найденной сущности (юр.лица)
            ci.BaseName as baseName,
            NULL as relatedINN,
            NULL as relatedEntityName,
            'contragent_direct_inn_match' as searchContext -- <<< Контекст поиска
        FROM CI_Contragent_test ci
        WHERE ci.INN IN (${innParams.join(', ')})
    `;

    // Запрос 2: Совпадения в CI_Employees.phOrgINN (поиск физ.лиц, работавших в организации)
    const employeeINNQuery = `
        SELECT
            ce.phOrgINN as contactUNID, -- ИНН организации (связанная сущность)
            ce.phOrgINN as contactINN, -- ИНН организации
            ce.fzFIO as contactNameShort,
            ce.fzFIO as contactNameFull,
            NULL as UrFiz,
            NULL as fIP,
            ce.fzUID as fzUID,
            NULL as cpUID,
            NULL as PersonUNID,
            ce.fzFIO as fzFIO,
            ce.phFunction as phFunction,
            ce.phEventType as phEventType,
            ce.phDate as phDate,
            NULL as PersonLastName,
            NULL as PersonFirstName,
            NULL as PersonMiddleName,
            NULL as PersonSNILS,
            'CI_Employees_test' as sourceTable, -- <<< Имя таблицы
            ce.fzUID as entityKey, -- <<< Ключ найденной сущности (физ.лица)
            ce.BaseName as baseName,
            ce.fzINN as relatedINN, -- ИНН самого сотрудника (физлица)
            ce.fzFIO as relatedEntityName,
            'employee_by_org_inn' as searchContext -- <<< Контекст поиска
        FROM CI_Employees_test ce
        WHERE ce.phOrgINN IN (${innParams.join(', ')}) -- Совпадение по ИНН организации
    `;

    // Запрос 3: Совпадения в CI_ContPersons.conINN (поиск контактных лиц организации)
    const contPersonINNQuery = `
        SELECT
            cip.conINN as contactUNID, -- ИНН организации (связанная сущность)
            cip.conINN as contactINN, -- ИНН организации
            cip.cpNameFull as contactNameShort,
            cip.cpNameFull as contactNameFull,
            NULL as UrFiz,
            NULL as fIP,
            NULL as fzUID,
            cip.cpUID as cpUID,
            NULL as PersonUNID,
            NULL as fzFIO,
            NULL as phFunction,
            NULL as phEventType,
            NULL as phDate,
            NULL as PersonLastName,
            NULL as PersonFirstName,
            NULL as PersonMiddleName,
            NULL as PersonSNILS,
            'CI_ContPersons_test' as sourceTable, -- <<< Имя таблицы
            cip.cpUID as entityKey, -- <<< Ключ найденной сущности (конт.лица)
            cip.BaseName as baseName,
            NULL as relatedINN,
            cip.cpNameFull as relatedEntityName,
            'contperson_by_org_inn' as searchContext -- <<< Контекст поиска
        FROM CI_ContPersons_test cip
        WHERE cip.conINN IN (${innParams.join(', ')}) -- Совпадение по ИНН организации
    `;

    // --- Запросы для поиска по ИНН физических лиц ---
    // Запрос 4: Совпадения в CI_Employees.fzINN (поиск организаций, где физлицо работает/работало)
    const employeeFzINNQuery = `
        SELECT
            ce.fzINN as contactUNID, -- ИНН физлица (связанная сущность)
            ce.fzINN as contactINN, -- ИНН физлица
            ce.phOrgName as contactNameShort, -- Название организации
            ce.phOrgName as contactNameFull, -- Название организации
            NULL as UrFiz,
            NULL as fIP,
            ce.fzUID as fzUID,
            NULL as cpUID,
            NULL as PersonUNID,
            ce.fzFIO as fzFIO,
            ce.phFunction as phFunction,
            ce.phEventType as phEventType,
            ce.phDate as phDate,
            NULL as PersonLastName,
            NULL as PersonFirstName,
            NULL as PersonMiddleName,
            NULL as PersonSNILS,
            'CI_Employees_test' as sourceTable, -- <<< Имя таблицы
            ce.phOrgINN as entityKey, -- <<< Ключ найденной сущности (организации)
            ce.BaseName as baseName,
            ce.fzINN as relatedINN, -- ИНН самого физлица
            ce.fzFIO as relatedEntityName,
            'employee_by_person_inn' as searchContext -- <<< Контекст поиска
        FROM CI_Employees_test ce
        WHERE ce.fzINN IN (${innParams.join(', ')}) -- Совпадение по ИНН физлица
    `;

    // Запрос 5: Совпадения в CF_Persons.INN -> CF_PrevWork (поиск организаций через предыдущие места работы)
    const personsINNQuery = `
        SELECT
            cp.INN as contactUNID, -- ИНН физлица (связанная сущность)
            cp.INN as contactINN, -- ИНН физлица
            cpw.Caption as contactNameShort, -- Название организации из CF_PrevWork
            cpw.Caption as contactNameFull, -- Название организации из CF_PrevWork
            NULL as UrFiz,
            NULL as fIP,
            NULL as fzUID,
            NULL as cpUID,
            cp.UNID as PersonUNID, -- PersonUNID физлица
            CONCAT(cp.LastName, ' ', cp.FirstName, ' ', COALESCE(cp.MiddleName, '')) as fzFIO, -- Собираем ФИО из CF_Persons
            NULL as phFunction,
            NULL as phEventType,
            NULL as phDate,
            cp.LastName as PersonLastName,
            cp.FirstName as PersonFirstName,
            cp.MiddleName as PersonMiddleName,
            cp.SNILS as PersonSNILS,
            'CF_PrevWork_test' as sourceTable, -- <<< Имя таблицы (таблица, откуда взяты данные org name/INN)
            cpw.OGRN as entityKey, -- <<< Ключ найденной сущности (организации)
            NULL as baseName,
            cp.INN as relatedINN, -- ИНН самого физлица
            CONCAT(cp.LastName, ' ', cp.FirstName, ' ', COALESCE(cp.MiddleName, '')) as relatedEntityName,
            'prevwork_by_person_inn_via_join' as searchContext -- <<< Контекст поиска
        FROM CF_Persons_test cp
        JOIN CF_PrevWork_test cpw ON cp.UNID = cpw.PersonUNID -- Соединяем с предыдущими местами работы
        WHERE cp.INN IN (${innParams.join(', ')}) -- Совпадение по ИНН физлица
    `;

    // Запрос 6: Совпадения в CF_Persons.INN (поиск самого физлица по ИНН, без JOIN)
    const personDirectINNQuery = `
        SELECT
            cp.INN as contactUNID, -- ИНН физлица (связанная сущность)
            cp.INN as contactINN, -- ИНН физлица
            CONCAT(cp.LastName, ' ', cp.FirstName, ' ', COALESCE(cp.MiddleName, '')) as contactNameShort, -- ФИО персоны как NameShort
            CONCAT(cp.LastName, ' ', cp.FirstName, ' ', COALESCE(cp.MiddleName, '')) as contactNameFull, -- ФИО персоны как NameFull
            NULL as UrFiz,
            NULL as fIP,
            NULL as fzUID,
            NULL as cpUID,
            cp.UNID as PersonUNID, -- PersonUNID физлица
            CONCAT(cp.LastName, ' ', cp.FirstName, ' ', COALESCE(cp.MiddleName, '')) as fzFIO, -- ФИО из CF_Persons
            NULL as phFunction,
            NULL as phEventType,
            NULL as phDate,
            cp.LastName as PersonLastName,
            cp.FirstName as PersonFirstName,
            cp.MiddleName as PersonMiddleName,
            cp.SNILS as PersonSNILS,
            'CF_Persons_test' as sourceTable, -- <<< Имя таблицы
            cp.UNID as entityKey, -- <<< Ключ найденной сущности (физ.лица)
            NULL as baseName,
            cp.INN as relatedINN, -- ИНН самого физлица
            CONCAT(cp.LastName, ' ', cp.FirstName, ' ', COALESCE(cp.MiddleName, '')) as relatedEntityName,
            'person_direct_inn_match' as searchContext -- <<< Контекст поиска
        FROM CF_Persons_test cp
        WHERE cp.INN IN (${innParams.join(', ')}) -- Совпадение по ИНН физлица (без JOIN)
    `;

    // Запрос 7: Совпадения в CF_PrevWork.INN (поиск людей, работавших в организации/ИП с данным ИНН)
    const prevWorkINNQuery = `
        SELECT
            cpw.INN as contactUNID, -- ИНН организации/ИП (связанная сущность)
            cpw.INN as contactINN, -- ИНН организации/ИП
            cpw.Caption as contactNameShort, -- Название организации/ИП из CF_PrevWork
            cpw.Caption as contactNameFull, -- Название организации/ИП из CF_PrevWork
            NULL as UrFiz,
            NULL as fIP,
            NULL as fzUID,
            NULL as cpUID,
            cpw.PersonUNID as PersonUNID, -- PersonUNID физлица, работавшего там
            NULL as fzFIO, -- Нет ФИО в CF_PrevWork, нужно будет подтягивать отдельно
            NULL as phFunction,
            NULL as phEventType,
            NULL as phDate,
            NULL as PersonLastName,
            NULL as PersonFirstName,
            NULL as PersonMiddleName,
            NULL as PersonSNILS,
            'CF_PrevWork_test' as sourceTable, -- <<< Имя таблицы
            cpw.PersonUNID as entityKey, -- <<< Ключ найденной сущности (физ.лица)
            NULL as baseName,
            cpw.INN as relatedINN, -- ИНН организации/ИП
            cpw.Caption as relatedEntityName,
            'prevwork_by_org_inn' as searchContext -- <<< Контекст поиска
        FROM CF_PrevWork_test cpw
        WHERE cpw.INN IN (${innParams.join(', ')}) -- Совпадение по ИНН организации/ИП (где человек работал)
    `;

    // Собираем все запросы в один UNION ALL
    return `${contragentINNQuery} UNION ALL ${employeeINNQuery} UNION ALL ${contPersonINNQuery} UNION ALL ${employeeFzINNQuery} UNION ALL ${personsINNQuery} UNION ALL ${personDirectINNQuery} UNION ALL ${prevWorkINNQuery}`;
}
