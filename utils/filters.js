// filters.js

function filterEntitiesByCompleteness(entities, keyField) {
    // Группируем сущности по уникальному ключу (например, INN)
    const grouped = new Map();

    entities.forEach(entity => {
        const key = entity[keyField];
        if (!key) return; // Пропускаем сущности без ключа

        if (!grouped.has(key)) {
            grouped.set(key, []);
        }
        grouped.get(key).push(entity);
    });

    const filteredEntities = [];

    grouped.forEach(entityGroup => {
        // Считаем заполненность для каждой сущности в группе
        const entitiesWithCompleteness = entityGroup.map(entity => {
            let completeness = 0;
            // eslint-disable-next-line no-unused-vars
            for (const [key, value] of Object.entries(entity)) {
                if (value !== null && value !== undefined && value !== '') {
                    completeness++;
                }
            }
            return { entity, completeness };
        });

        // Сортируем по заполненности (по убыванию) и берем первую (наиболее полную)
        entitiesWithCompleteness.sort((a, b) => b.completeness - a.completeness);
        const mostCompleteEntity = entitiesWithCompleteness[0].entity;

        // console.log(`Выбрана наиболее полная сущность для ключа ${mostCompleteEntity[keyField]} с ${entitiesWithCompleteness[0].completeness} заполненными полями.`);
        filteredEntities.push(mostCompleteEntity);
    });

    return filteredEntities;
}

export {
    filterEntitiesByCompleteness
};