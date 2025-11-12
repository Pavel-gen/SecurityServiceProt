import sql from 'mssql'
import 'dotenv/config';
import { config } from '../config/dbConfig.js';
// --- ИМПОРТИРУЕМ cleanPhone ---
import { cleanPhone } from '../utils/helper.js'; // Путь к utils.helper.js может отличаться
// ---

// Приводит поля INN, UNID, fzUID, cpUID, PersonUNID к единому формату
function normalizeEntity(entity) {
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

// --- ВСПОМОГАТЕЛЬНАЯ ФУНКЦИЯ: Определение типа query ---
function determineQueryType(query) {
    // Проверка на email (простая проверка)
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (emailRegex.test(query)) {
        return { type: 'email', value: query.toLowerCase() };
    }

    // Проверка на телефон (убираем все, кроме цифр)
    const cleanedPhone = cleanPhone(query);
    if (/^(7|8)?\d{10}$/.test(cleanedPhone)) {
        const normalizedForSearch = cleanedPhone.length === 10 ? '7' + cleanedPhone : cleanedPhone.replace(/^8/, '7');
        return { type: 'phone', value: normalizedForSearch };
    }

    // Проверка на OGRN (13 цифр)
    if (/^\d{13}$/.test(query)) {
        return { type: 'ogrn', value: query };
    }

    // Если не email, не телефон, не ОГРН, считаем это обычным текстовым запросом
    return { type: 'text', value: query };
}

async function fetchLocalData(query) {
    console.log(`Получен запрос: ${query}`);
    const queryInfo = determineQueryType(query);
    console.log(`Тип запроса: ${queryInfo.type}, Значение для поиска: ${queryInfo.value}`);

    // Подключение к БД
    const pool = await sql.connect(config);

    // Подготовка параметров для SQL запросов
    const request = new sql.Request(pool);

    // --- Подготовка параметров в зависимости от типа query ---
    let queryParam = queryInfo.value; // Для точного совпадения (например, OGRN) или нормализованного телефона/email
    let phoneParam = null;
    let emailParam = null;
    let textParam = null; // Это будет '%query%' для LIKE
    let ogrnParam = null;

    if (queryInfo.type === 'phone') {
        phoneParam = queryInfo.value; // Нормализованный телефон для точного/частичного поиска
        textParam = `%${query}%`; // Оригинальный query для 'LIKE query' перестраховки
    } else if (queryInfo.type === 'email') {
        emailParam = `%${queryInfo.value}%`; // Email для точного/частичного поиска (обычно точное)
        textParam = `%${query}%`; // Оригинальный query для 'LIKE query' перестраховки
    } else if (queryInfo.type === 'ogrn') {
        ogrnParam = queryInfo.value; // OGRN для точного поиска
        textParam = `%${query}%`; // Оригинальный query для 'LIKE query' перестраховки
    } else { // text (включая ИНН, ФИО, UNID, Caption, Address, и т.д.)
        textParam = `%${query}%`;
    }

    // --- ВСЕГДА ДОБАВЛЯЕМ ВСЕ ПАРАМЕТРЫ ---
    request.input('query', sql.VarChar, textParam); // Для 'LIKE query' перестраховки
    request.input('phoneQuery', sql.VarChar, phoneParam); // Для точного/нормализованного поиска телефона
    request.input('emailQuery', sql.VarChar, emailParam); // Для поиска email
    request.input('ogrnQuery', sql.VarChar, ogrnParam);   // Для точного поиска OGRN
    // ---


    // --- ОБНОВЛЕННЫЕ ЗАПРОСЫ С УЧЕТОМ ТЕЛЕФОНА, EMAIL, АДРЕСОВ, OGRN И ПЕРЕСТРАХОВКИ 'LIKE QUERY' ---
    // Запрос 1: Поиск в CI_Contragent_test
    const contragentQuery = `
        SELECT
            UNID, ConCode, INN, KPP, OGRN, NameShort, NameFull, PhoneNum, eMail, UrFiz, fIP, AddressUr, AddressUFakt, fSZ, fIP as fIP_CG,
            BaseName -- Добавляем BaseName
        FROM CI_Contragent_test
        WHERE
            -- Основной поиск по тексту, включая телефоны и email (перестраховка)
            (@query IS NOT NULL AND (INN LIKE @query OR NameShort LIKE @query OR NameFull LIKE @query OR UNID LIKE @query OR OGRN LIKE @query OR AddressUr LIKE @query OR AddressUFakt LIKE @query OR PhoneNum LIKE @query OR eMail LIKE @query))
            OR
            -- Поиск по нормализованному телефону (если query был телефоном)
            (@phoneQuery IS NOT NULL AND PhoneNum LIKE @phoneQuery) -- PhoneNum может быть не нормализована, но ищем вхождение нормализованного
            OR
            -- Поиск по email (если query был email)
            (@emailQuery IS NOT NULL AND eMail LIKE @emailQuery)
            OR
            -- Поиск по OGRN (если query был OGRN)
            (@ogrnQuery IS NOT NULL AND OGRN = @ogrnQuery) -- Точное совпадение для OGRN
    `;

    // Запрос 2: Поиск в CI_Employees_test
    const employeeQuery = `
        SELECT
            fzUID, fzFIO, fzCode, fzDateB, fzAddress, fzAddressF, fzPhone, fzMail, fzINN,
            emUID, phOrgUID, phOrgINN, phOrgName, phDep, phFunction, phEventType, phContractType, phDate, phRegistrator, phRegUID,
            fzPhoneM,
            BaseName -- Добавляем BaseName
        FROM CI_Employees_test
        WHERE
            -- Основной поиск по тексту, включая телефоны и email (перестраховка)
            (@query IS NOT NULL AND (fzFIO LIKE @query OR fzUID LIKE @query OR phOrgINN LIKE @query OR fzAddress LIKE @query OR fzAddressF LIKE @query OR fzPhone LIKE @query OR fzMail LIKE @query OR fzPhoneM LIKE @query))
            OR
            -- Поиск по нормализованному телефону (если query был телефоном)
            (@phoneQuery IS NOT NULL AND (fzPhone LIKE @phoneQuery OR fzPhoneM LIKE @phoneQuery))
            OR
            -- Поиск по email (если query был email)
            (@emailQuery IS NOT NULL AND fzMail LIKE @emailQuery)
    `;

    // Запрос 3: Поиск в CI_ContPersons_test
    const contPersonQuery = `
        SELECT
            cpUID, conUID, conCode, conINN, cpNameFull, cpName1, cpName2, cpName3, cpDateB, cpFunction, cpVid,
            cpPhoneMob, cpPhoneMobS, cpPhoneWork, cpMail, cpAddress, cpCountry, cpReg, cpTown,
            BaseName -- Добавляем BaseName
        FROM CI_ContPersons_test
        WHERE
            -- Основной поиск по тексту, включая телефоны и email (перестраховка)
            (@query IS NOT NULL AND (cpNameFull LIKE @query OR cpUID LIKE @query OR conINN LIKE @query OR cpAddress LIKE @query OR cpTown LIKE @query OR cpPhoneMob LIKE @query OR cpPhoneMobS LIKE @query OR cpPhoneWork LIKE @query OR cpMail LIKE @query))
            OR
            -- Поиск по нормализованному телефону (если query был телефоном)
            (@phoneQuery IS NOT NULL AND (cpPhoneMob LIKE @phoneQuery OR cpPhoneMobS LIKE @phoneQuery OR cpPhoneWork LIKE @phoneQuery))
            OR
            -- Поиск по email (если query был email)
            (@emailQuery IS NOT NULL AND cpMail LIKE @emailQuery)
    `;

    // Запрос 4: Поиск в CF_PrevWork_test
    const prevWorkQuery = `
        SELECT
            PersonUNID, INN, OGRN, Caption, Phone, EMail, WorkPeriod
        FROM CF_PrevWork_test
        WHERE
            -- Основной поиск по тексту, включая телефоны и email (перестраховка)
            (@query IS NOT NULL AND (Caption LIKE @query OR PersonUNID LIKE @query OR INN LIKE @query OR Phone LIKE @query OR EMail LIKE @query))
            OR
            -- Поиск по нормализованному телефону (если query был телефоном)
            (@phoneQuery IS NOT NULL AND Phone LIKE @phoneQuery)
            OR
            -- Поиск по email (если query был email)
            (@emailQuery IS NOT NULL AND EMail LIKE @emailQuery)
            OR
            -- Поиск по OGRN (если query был OGRN)
            (@ogrnQuery IS NOT NULL AND OGRN = @ogrnQuery) -- Точное совпадение для OGRN
    `;

    // Запрос 5: Поиск в CF_Persons_test
    const personQuery = `
        SELECT
            UNID, INN, SNILS, FirstName, LastName, MiddleName, BirthDate, RegAddressPassport, RegAddressForm, ResAddressForm, State
        FROM CF_Persons_test
        WHERE
            -- Основной поиск по тексту (перестраховка)
            -- Телефоны/Emails в CF_Persons_test отсутствуют, но адреса могут содержать
            (@query IS NOT NULL AND (LastName LIKE @query OR FirstName LIKE @query OR MiddleName LIKE @query OR UNID LIKE @query OR INN LIKE @query OR RegAddressPassport LIKE @query OR RegAddressForm LIKE @query OR ResAddressForm LIKE @query))
            -- Нет нормализованного поиска телефона/email в этой таблице, так как полей нет
    `;

    // Запрос 6: Поиск в CF_Contacts_test
    const contactQuery = `
        SELECT
            PersonUNID, ContactType, Contact
        FROM CF_Contacts_test
        WHERE
            -- Основной поиск по тексту (перестраховка)
            (@query IS NOT NULL AND (PersonUNID LIKE @query OR Contact LIKE @query))
            OR
            -- Поиск по нормализованному телефону/email в Contact (если query был телефоном/email)
            (@phoneQuery IS NOT NULL AND Contact LIKE @phoneQuery) -- Предполагаем, что контакт может быть телефоном
            OR
            (@emailQuery IS NOT NULL AND Contact LIKE @emailQuery) -- Предполагаем, что контакт может быть email
    `;

    // Выполнение запросов параллельно
    const [contragentResult, employeeResult, contPersonResult, prevWorkResult, personResult, contactResult] = await Promise.all([
        request.query(contragentQuery),
        request.query(employeeQuery),
        request.query(contPersonQuery),
        request.query(prevWorkQuery),
        request.query(personQuery),
        request.query(contactQuery)
    ]);

    // --- НОРМАЛИЗАЦИЯ И ОБЪЕДИНЕНИЕ РЕЗУЛЬТАТОВ ---
    const contragents = contragentResult.recordset.map(row => {
        const normalized = normalizeEntity({
            UNID: row.UNID,
            ConCode: row.ConCode,
            INN: row.INN,
            KPP: row.KPP,
            OGRN: row.OGRN,
            NameShort: row.NameShort,
            NameFull: row.NameFull,
            PhoneNum: row.PhoneNum,
            eMail: row.eMail,
            UrFiz: row.UrFiz,
            fIP: row.fIP_CG, // Используем псевдоним
            AddressUr: row.AddressUr,
            AddressUFakt: row.AddressUFakt,
            fSZ: row.fSZ,
            type: 'juridical', // или 'ip' в зависимости от fIP - это будет перезаписано
            // --- ДОБАВЛЯЕМ ИНФОРМАЦИЮ ОБ ИСТОЧНИКЕ ---
            source: 'local',
            sourceTable: 'CI_Contragent_test',
            baseName: row.BaseName
            // ---
        });
        // --- ИСПРАВЛЕННАЯ ЛОГИКА ОПРЕДЕЛЕНИЯ ТИПА ---
        // Проверяем fIP (предполагаем, что 1 означает ИП, 0 - нет)
        if (normalized.fIP === 1 || normalized.fIP === true) { // Явно проверяем 1 или true
             normalized.type = 'ip';
        } else if (normalized.UrFiz === 1) {
             normalized.type = 'juridical';
        } else if (normalized.UrFiz === 2) {
             normalized.type = 'physical';
        }
        // Если ни одно условие не сработало, останется 'juridical' по умолчанию (или можно оставить как есть, если это редкий случай)
        // ---
        return normalized;
    });

    const employees = employeeResult.recordset.map(row => normalizeEntity({
        fzUID: row.fzUID,
        fzFIO: row.fzFIO,
        fzCode: row.fzCode,
        fzDateB: row.fzDateB,
        fzAddress: row.fzAddress,
        fzAddressF: row.fzAddressF,
        fzPhone: row.fzPhone,
        fzMail: row.fzMail,
        fzINN: row.fzINN,
        emUID: row.emUID,
        phOrgUID: row.phOrgUID,
        phOrgINN: row.phOrgINN,
        phOrgName: row.phOrgName,
        phDep: row.phDep,
        phFunction: row.phFunction,
        phEventType: row.phEventType,
        phContractType: row.phContractType,
        phDate: row.phDate,
        phRegistrator: row.phRegistrator,
        phRegUID: row.phRegUID,
        fzPhoneM: row.fzPhoneM,
        type: 'physical',
        // --- ДОБАВЛЯЕМ ИНФОРМАЦИЮ ОБ ИСТОЧНИКЕ ---
        source: 'local',
        sourceTable: 'CI_Employees_test',
        baseName: row.BaseName
        // ---
    }));

    const contPersons = contPersonResult.recordset.map(row => normalizeEntity({
        cpUID: row.cpUID,
        conUID: row.conUID,
        conCode: row.conCode,
        conINN: row.conINN,
        cpNameFull: row.cpNameFull,
        cpName1: row.cpName1,
        cpName2: row.cpName2,
        cpName3: row.cpName3,
        cpDateB: row.cpDateB,
        cpFunction: row.cpFunction,
        cpVid: row.cpVid,
        cpPhoneMob: row.cpPhoneMob,
        cpPhoneMobS: row.cpPhoneMobS,
        cpPhoneWork: row.cpPhoneWork,
        cpMail: row.cpMail,
        cpAddress: row.cpAddress,
        cpCountry: row.cpCountry,
        cpReg: row.cpReg,
        cpTown: row.cpTown,
        type: 'physical',
        // --- ДОБАВЛЯЕМ ИНФОРМАЦИЮ ОБ ИСТОЧНИКЕ ---
        source: 'local',
        sourceTable: 'CI_ContPersons_test',
        baseName: row.BaseName
        // ---
    }));

    const prevWorks = prevWorkResult.recordset.map(row => normalizeEntity({
        PersonUNID: row.PersonUNID,
        INN: row.INN,
        OGRN: row.OGRN,
        Caption: row.Caption,
        Phone: row.Phone,
        eMail: row.EMail, // Обратите внимание на исходное имя столбца в запросе
        WorkPeriod: row.WorkPeriod,
        type: 'prevwork', // Указываем тип для удобства
        // --- ДОБАВЛЯЕМ ИНФОРМАЦИЮ ОБ ИСТОЧНИКЕ ---
        source: 'local',
        sourceTable: 'CF_PrevWork_test',
        baseName: null // CF_PrevWork_test не имеет BaseName
        // ---
    }));

    const persons = personResult.recordset.map(row => {
        // Собираем ФИО из отдельных полей
        const firstName = row.FirstName || '';
        const lastName = row.LastName || '';
        const middleName = row.MiddleName || '';
        const fullName = `${lastName} ${firstName} ${middleName}`.trim();

        // Создаем сущность, добавляя NameShort и NameFull
        const entity = normalizeEntity({
            UNID: row.UNID,
            INN: row.INN,
            SNILS: row.SNILS,
            FirstName: row.FirstName,
            LastName: row.LastName,
            MiddleName: row.MiddleName,
            BirthDate: row.BirthDate,
            RegAddressPassport: row.RegAddressPassport,
            RegAddressForm: row.RegAddressForm,
            ResAddressForm: row.ResAddressForm,
            State: row.State,
            // --- ДОБАВЛЯЕМ NameShort и NameFull ---
            NameShort: fullName || row.INN, // Если ФИО не удалось собрать, используем ИНН как fallback
            NameFull: fullName || row.INN, // Аналогично для NameFull
            // ---
            type: 'physical', // Указываем тип для удобства
            // --- ДОБАВЛЯЕМ ИНФОРМАЦИЮ ОБ ИСТОЧНИКЕ ---
            source: 'local',
            sourceTable: 'CF_Persons_test',
            baseName: null // CF_Persons_test не имеет BaseName
            // ---
        });

        return entity;
    });


    const contacts = contactResult.recordset.map(row => ({
        PersonUNID: row.PersonUNID,
        ContactType: row.ContactType,
        Contact: row.Contact,
        type: 'contact', // Указываем тип для удобства
        // --- ДОБАВЛЯЕМ ИНФОРМАЦИЮ ОБ ИСТОЧНИКЕ ---
        source: 'local',
        sourceTable: 'CF_Contacts_test',
        baseName: null // CF_Contacts_test не имеет BaseName
        // ---
    }));

    // Объединяем все результаты
    const allLocalResults = [...contragents, ...employees, ...contPersons, ...prevWorks, ...persons, ...contacts];

    // --- СОЗДАНИЕ СПИСКА ЦЕЛЕВЫХ СУЩНОСТЕЙ ДЛЯ ПОИСКА СВЯЗЕЙ ---
    // Фильтруем сущности, у которых есть ключ (UNID, fzUID, cpUID, PersonUNID) и ИНН
    const allTargetEntitiesForConnections = allLocalResults.filter(entity => {
        const entityKey = entity.UNID || entity.fzUID || entity.cpUID || entity.PersonUNID;
        // Используем нормализованное поле INN
        return entityKey && entity.INN && entity.INN.trim() !== '';
    });

    console.log(`Найдено ${allTargetEntitiesForConnections.length} локальных сущностей с ключом и INN для поиска связей.`);
    console.log(`Найдено ${allLocalResults.length} локальных сущностей по параметрам (ИНН, ФИО, UNID, Caption, Телефон, Email, Адрес, OGRN).`);

    return {
        localResults: allLocalResults,
        allTargetEntitiesForConnections: allTargetEntitiesForConnections
    };
}

export { fetchLocalData };
