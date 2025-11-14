// queries/inn.queries.js

function buildINNQuery(innArray) {
    if (innArray.length === 0) return 'SELECT 1 as dummy WHERE 1=0';

    const innParams = innArray.map((inn, index) => `@inn${index}`);

    // --- ЗАПРОСЫ ДЛЯ ЮР.ЛИЦ ---
    const contragentINNQuery = `
        SELECT
            ci.UNID as contactUNID,
            ci.INN as contactINN,
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
            'CI_Contragent_test' as sourceTable, -- ЕДИНООБРАЗНОЕ НАЗВАНИЕ
            ci.UNID as entityKey,
            ci.BaseName as baseName,
            NULL as relatedINN,
            NULL as relatedEntityName
        FROM CI_Contragent_test ci
        WHERE ci.INN IN (${innParams.join(', ')})
    `;

    const employeeINNQuery = `
        SELECT
            ce.phOrgINN as contactUNID,
            ce.phOrgINN as contactINN,
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
            'CI_Employees_test' as sourceTable, -- ЕДИНООБРАЗНОЕ НАЗВАНИЕ
            ce.fzUID as entityKey,
            ce.BaseName as baseName,
            ce.fzINN as relatedINN,
            ce.fzFIO as relatedEntityName
        FROM CI_Employees_test ce
        WHERE ce.phOrgINN IN (${innParams.join(', ')})
    `;

    const contPersonINNQuery = `
        SELECT
            cip.conINN as contactUNID,
            cip.conINN as contactINN,
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
            'CI_ContPersons_test' as sourceTable, -- ЕДИНООБРАЗНОЕ НАЗВАНИЕ
            cip.cpUID as entityKey,
            cip.BaseName as baseName,
            NULL as relatedINN,
            cip.cpNameFull as relatedEntityName
        FROM CI_ContPersons_test cip
        WHERE cip.conINN IN (${innParams.join(', ')})
    `;

    // --- ЗАПРОСЫ ДЛЯ ФИЗ.ЛИЦ ---
    const employeeFzINNQuery = `
        SELECT
            ce.fzINN as contactUNID,
            ce.fzINN as contactINN,
            ce.phOrgName as contactNameShort,
            ce.phOrgName as contactNameFull,
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
            'CI_Employees_test' as sourceTable, -- ЕДИНООБРАЗНОЕ НАЗВАНИЕ
            ce.phOrgINN as entityKey,
            ce.BaseName as baseName,
            ce.fzINN as relatedINN,
            ce.fzFIO as relatedEntityName
        FROM CI_Employees_test ce
        WHERE ce.fzINN IN (${innParams.join(', ')})
    `;

    const personsINNQuery = `
        SELECT
            cp.INN as contactUNID,
            cp.INN as contactINN,
            cpw.Caption as contactNameShort,
            cpw.Caption as contactNameFull,
            NULL as UrFiz,
            NULL as fIP,
            NULL as fzUID,
            NULL as cpUID,
            cp.UNID as PersonUNID,
            CONCAT(cp.LastName, ' ', cp.FirstName, ' ', COALESCE(cp.MiddleName, '')) as fzFIO,
            NULL as phFunction,
            NULL as phEventType,
            NULL as phDate,
            cp.LastName as PersonLastName,
            cp.FirstName as PersonFirstName,
            cp.MiddleName as PersonMiddleName,
            cp.SNILS as PersonSNILS,
            'CF_Persons_test' as sourceTable, -- ЕДИНООБРАЗНОЕ НАЗВАНИЕ
            cpw.OGRN as entityKey,
            NULL as baseName,
            cp.INN as relatedINN,
            CONCAT(cp.LastName, ' ', cp.FirstName, ' ', COALESCE(cp.MiddleName, '')) as relatedEntityName
        FROM CF_Persons_test cp
        JOIN CF_PrevWork_test cpw ON cp.UNID = cpw.PersonUNID
        WHERE cp.INN IN (${innParams.join(', ')})
    `;

    const personDirectINNQuery = `
        SELECT
            cp.INN as contactUNID,
            cp.INN as contactINN,
            CONCAT(cp.LastName, ' ', cp.FirstName, ' ', COALESCE(cp.MiddleName, '')) as contactNameShort,
            CONCAT(cp.LastName, ' ', cp.FirstName, ' ', COALESCE(cp.MiddleName, '')) as contactNameFull,
            NULL as UrFiz,
            NULL as fIP,
            NULL as fzUID,
            NULL as cpUID,
            cp.UNID as PersonUNID,
            CONCAT(cp.LastName, ' ', cp.FirstName, ' ', COALESCE(cp.MiddleName, '')) as fzFIO,
            NULL as phFunction,
            NULL as phEventType,
            NULL as phDate,
            cp.LastName as PersonLastName,
            cp.FirstName as PersonFirstName,
            cp.MiddleName as PersonMiddleName,
            cp.SNILS as PersonSNILS,
            'CF_Persons_test' as sourceTable, -- ЕДИНООБРАЗНОЕ НАЗВАНИЕ
            cp.UNID as entityKey,
            NULL as baseName,
            cp.INN as relatedINN,
            CONCAT(cp.LastName, ' ', cp.FirstName, ' ', COALESCE(cp.MiddleName, '')) as relatedEntityName
        FROM CF_Persons_test cp
        WHERE cp.INN IN (${innParams.join(', ')})
    `;

    const prevWorkINNQuery = `
        SELECT
            cpw.INN as contactUNID,
            cpw.INN as contactINN,
            cpw.Caption as contactNameShort,
            cpw.Caption as contactNameFull,
            NULL as UrFiz,
            NULL as fIP,
            NULL as fzUID,
            NULL as cpUID,
            cpw.PersonUNID as PersonUNID,
            NULL as fzFIO,
            NULL as phFunction,
            NULL as phEventType,
            NULL as phDate,
            NULL as PersonLastName,
            NULL as PersonFirstName,
            NULL as PersonMiddleName,
            NULL as PersonSNILS,
            'CF_PrevWork_test' as sourceTable, -- ЕДИНООБРАЗНОЕ НАЗВАНИЕ
            cpw.PersonUNID as entityKey,
            NULL as baseName,
            cpw.INN as relatedINN,
            cpw.Caption as relatedEntityName
        FROM CF_PrevWork_test cpw
        WHERE cpw.INN IN (${innParams.join(', ')})
    `;

    // Собираем все запросы
    return `
        ${contragentINNQuery} 
        UNION ALL ${employeeINNQuery} 
        UNION ALL ${contPersonINNQuery} 
        UNION ALL ${employeeFzINNQuery} 
        UNION ALL ${personsINNQuery} 
        UNION ALL ${personDirectINNQuery} 
        UNION ALL ${prevWorkINNQuery}
    `;
}

export { buildINNQuery };