function buildINNQuery(innArray) {
    if (innArray.length === 0) return 'SELECT 1 as dummy WHERE 1=0'; // Пустой запрос, если нет ИНН

    const innParams = innArray.map((inn, index) => `@inn${index}`);

    // --- Существующие запросы для юр.лиц ---
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
            'contragent' as sourceTable,
            ci.UNID as entityKey,
            ci.BaseName as baseName,
            NULL as relatedINN, -- Для юр.лиц связь по ИНН -> юр.лицо, relatedINN не нужно
            NULL as relatedEntityName -- Имя связанной сущности (для юр.лиц - не применимо в этом контексте)
        FROM CI_Contragent_test ci
        WHERE ci.INN IN (${innParams.join(', ')})
    `;

    const employeeINNQuery = `
        SELECT
            ce.phOrgINN as contactUNID, -- UNID организации (связанная сущность)
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
            NULL as PersonLastName, -- Не из CF_Persons
            NULL as PersonFirstName,
            NULL as PersonMiddleName,
            NULL as PersonSNILS,
            'employee' as sourceTable,
            ce.fzUID as entityKey, -- Ключ сотрудника
            ce.BaseName as baseName,
            ce.fzINN as relatedINN, -- ИНН самого сотрудника (физлица) - это target INN
            ce.fzFIO as relatedEntityName -- Имя физлица
        FROM CI_Employees_test ce
        WHERE ce.phOrgINN IN (${innParams.join(', ')}) -- Совпадение по ИНН организации
    `;

    const contPersonINNQuery = `
        SELECT
            cip.conINN as contactUNID, -- UNID организации (связанная сущность)
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
            NULL as PersonLastName, -- Не из CF_Persons
            NULL as PersonFirstName,
            NULL as PersonMiddleName,
            NULL as PersonSNILS,
            'contperson' as sourceTable,
            cip.cpUID as entityKey, -- Ключ контактного лица
            cip.BaseName as baseName,
            NULL as relatedINN, -- Для контактных лиц по ИНН организации - relatedINN не применимо как target INN
            cip.cpNameFull as relatedEntityName -- Имя контактного лица
        FROM CI_ContPersons_test cip
        WHERE cip.conINN IN (${innParams.join(', ')}) -- Совпадение по ИНН организации
    `;

    // --- НОВЫЕ запросы для поиска по ИНН физических лиц ---
    // Запрос 5: Совпадения в CI_Employees.fzINN (поиск организаций, где физлицо работает/работало)
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
            NULL as PersonLastName, -- Не из CF_Persons в этом запросе
            NULL as PersonFirstName,
            NULL as PersonMiddleName,
            NULL as PersonSNILS,
            'employee_by_person_inn' as sourceTable, -- Новый тип источника
            ce.phOrgINN as entityKey, -- Ключ организации
            ce.BaseName as baseName,
            ce.fzINN as relatedINN, -- ИНН самого физлица - это target INN
            ce.fzFIO as relatedEntityName -- Имя физлица (из CI_Employees)
        FROM CI_Employees_test ce
        WHERE ce.fzINN IN (${innParams.join(', ')}) -- Совпадение по ИНН физлица
    `;

    // Запрос 6: Совпадения в CF_Persons.INN (поиск организаций через предыдущие места работы)
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
            NULL as phFunction, -- Нет в CF_PrevWork
            NULL as phEventType, -- Нет в CF_PrevWork
            NULL as phDate, -- Нет в CF_PrevWork
            cp.LastName as PersonLastName, -- Фамилия из CF_Persons
            cp.FirstName as PersonFirstName, -- Имя из CF_Persons
            cp.MiddleName as PersonMiddleName, -- Отчество из CF_Persons
            cp.SNILS as PersonSNILS, -- SNILS из CF_Persons
            'person_by_inn_via_prevwork' as sourceTable, -- Новый тип источника
            cpw.OGRN as entityKey, -- Ключ организации (можно использовать OGRN или INN)
            NULL as baseName, -- BaseName возможно нет в CF_PrevWork_test
            cp.INN as relatedINN, -- ИНН самого физлица - это target INN
            CONCAT(cp.LastName, ' ', cp.FirstName, ' ', COALESCE(cp.MiddleName, '')) as relatedEntityName -- Имя физлица
        FROM CF_Persons_test cp
        JOIN CF_PrevWork_test cpw ON cp.UNID = cpw.PersonUNID -- Соединяем с предыдущими местами работы
        WHERE cp.INN IN (${innParams.join(', ')}) -- Совпадение по ИНН физлица
    `;

    // --- НОВЫЙ запрос: Совпадения в CF_Persons.INN (поиск самого физлица по ИНН, без JOIN на prevwork) ---
    // Этот запрос находит *саму персону* по ИНН, даже если у неё нет prevwork.
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
            cp.LastName as PersonLastName, -- Фамилия из CF_Persons
            cp.FirstName as PersonFirstName, -- Имя из CF_Persons
            cp.MiddleName as PersonMiddleName, -- Отчество из CF_Persons
            cp.SNILS as PersonSNILS, -- SNILS из CF_Persons
            'person_direct_inn_match' as sourceTable, -- Новый тип источника
            cp.UNID as entityKey, -- Ключ самой персоны
            NULL as baseName, -- BaseName возможно нет в CF_Persons_test
            cp.INN as relatedINN, -- ИНН самого физлица - это target INN
            CONCAT(cp.LastName, ' ', cp.FirstName, ' ', COALESCE(cp.MiddleName, '')) as relatedEntityName -- Имя физлица
        FROM CF_Persons_test cp
        WHERE cp.INN IN (${innParams.join(', ')}) -- Совпадение по ИНН физлица (без JOIN)
    `;

    // --- НОВЫЙ запрос: Совпадения в CF_PrevWork.INN (поиск людей, работавших в организации/ИП с данным ИНН) ---
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
            NULL as PersonLastName, -- Не из CF_Persons в этом запросе
            NULL as PersonFirstName,
            NULL as PersonMiddleName,
            NULL as PersonSNILS,
            'prevwork_by_org_inn' as sourceTable, -- Новый тип источника
            cpw.PersonUNID as entityKey, -- Ключ физлица (человек, работавший в этой организации)
            NULL as baseName, -- BaseName возможно нет в CF_PrevWork_test
            cpw.INN as relatedINN, -- ИНН организации/ИП - это target INN
            cpw.Caption as relatedEntityName -- Название организации/ИП
        FROM CF_PrevWork_test cpw
        WHERE cpw.INN IN (${innParams.join(', ')}) -- Совпадение по ИНН организации/ИП (где человек работал)
    `;

    // Собираем все запросы в один UNION ALL
    return `${contragentINNQuery} UNION ALL ${employeeINNQuery} UNION ALL ${contPersonINNQuery} UNION ALL ${employeeFzINNQuery} UNION ALL ${personsINNQuery} UNION ALL ${personDirectINNQuery} UNION ALL ${prevWorkINNQuery}`;
}

export {buildINNQuery}