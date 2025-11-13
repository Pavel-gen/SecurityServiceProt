// find_connections.js
import { findConnectionsByINN } from "./connectionByINNService.js";
import { normalizeEntity, getEntityKey, determineEntityType } from '../utils/helper.js';

async function findConnections(entities) {
    console.log("Запуск findConnections");
    
    // Нормализуем сущности
    const normalizedEntities = entities.map(normalizeEntity);
    console.log(`Найдено ${normalizedEntities.length} нормализованных сущностей`);

    // Создаем карту для быстрого доступа
    const entitiesMap = new Map();
    normalizedEntities.forEach(entity => {
        const key = getEntityKey(entity);
        if (key) entitiesMap.set(key, entity);
    });

    // Ищем связи только по ИНН (как в текущей версии)
    const innConnectionsMap = await findConnectionsByINN(normalizedEntities);
    
    // Объединяем результаты
    const resultsWithConnections = await buildFinalResults(
        normalizedEntities, 
        entitiesMap, 
        innConnectionsMap
    );

    console.log(`Возвращаем ${resultsWithConnections.length} сущностей с связями`);
    return resultsWithConnections;
}

async function buildFinalResults(normalizedEntities, entitiesMap, innConnectionsMap) {
    const allEntities = new Map();
    
    // Добавляем исходные сущности
    normalizedEntities.forEach(entity => {
        const key = getEntityKey(entity);
        if (key) allEntities.set(key, { ...entity, connections: [] });
    });
    
    // Добавляем связанные сущности из connectionsMap
    for (const [entityKey, connectionsByINN] of innConnectionsMap) {
        for (const [inn, connections] of Object.entries(connectionsByINN)) {
            connections.forEach(connection => {
                const connectedEntity = connection.connectedEntity;
                const connectedKey = getEntityKey(connectedEntity);
                
                if (connectedKey && !allEntities.has(connectedKey)) {
                    allEntities.set(connectedKey, {
                        ...normalizeEntity(connectedEntity),
                        connections: []
                    });
                }
                
                // Добавляем связь к исходной сущности
                if (allEntities.has(entityKey)) {
                    const entity = allEntities.get(entityKey);
                    const existingConnectionGroup = entity.connections.find(
                        conn => conn.type === 'inn' && conn.contact === inn
                    );
                    
                    if (existingConnectionGroup) {
                        existingConnectionGroup.connections.push(connection);
                    } else {
                        entity.connections.push({
                            contact: inn,
                            type: 'inn',
                            subtype: 'inn_match',
                            connections: [connection]
                        });
                    }
                }
            });
        }
    }
    
    // Обрабатываем связи предыдущих мест работы
    await processPrevWorkConnections(allEntities, innConnectionsMap);
    
    // Преобразуем в массив и считаем количество связей
    return Array.from(allEntities.values()).map(entity => ({
        ...entity,
        connectionsCount: entity.connections.length
    }));
}

async function processPrevWorkConnections(allEntities, innConnectionsMap) {
    // Обработка связей через предыдущие места работы
    for (const entity of allEntities.values()) {
        if (entity.type === 'juridical' && entity.INN) {
            const prevWorkEntities = Array.from(allEntities.values()).filter(
                e => e.type === 'prevwork' && e.INN === entity.INN && e.PersonUNID
            );
            
            for (const prevWorkEntity of prevWorkEntities) {
                const prevWorkKey = getEntityKey(prevWorkEntity);
                if (prevWorkKey && innConnectionsMap.has(prevWorkKey)) {
                    const prevWorkConnections = innConnectionsMap.get(prevWorkKey);
                    
                    for (const [inn, connections] of Object.entries(prevWorkConnections)) {
                        let connectionGroup = entity.connections.find(
                            conn => conn.type === 'inn' && conn.contact === inn
                        );
                        
                        if (!connectionGroup) {
                            connectionGroup = {
                                contact: inn,
                                type: 'inn',
                                subtype: 'inn_match',
                                connections: []
                            };
                            entity.connections.push(connectionGroup);
                        }
                        
                        connectionGroup.connections.push(...connections);
                    }
                }
            }
        }
    }
}

export { findConnections };