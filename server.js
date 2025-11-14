// server.js
import express from 'express'
import path from 'path'
import  { findConnections} from './services/connectionService.js';
import { fetchDeltaData } from './services/deltaService.js';
import { fetchLocalData } from './services/dataService.js'
import { fileURLToPath } from 'url';


const app = express();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

app.use(express.static(path.join(__dirname, 'public')));
app.use(express.json());


// --- ОСНОВНОЙ ЭНДПОИНТ ---
app.post('/api/search', async (req, res) => {
    const { query } = req.body;
    if (!query || query.trim().length < 3) {
        return res.json({ juridical: [], physical: [], ip: [], delta_results: [] });
    }

    try {
        // Шаг 1: Получить локальные данные
        // eslint-disable-next-line no-unused-vars
        const { localResults, allTargetEntitiesForConnections } = await fetchLocalData(query);
       

        // Шаг 2: Получить данные из Delta
        const deltaResults = await fetchDeltaData(query);

        console.log("Что нашла дельта безопасности: ", deltaResults);


        // Шаг 4: Найти связи для объединённых результатов
        const resultsWithConnections = await findConnections([...deltaResults, ...localResults]);

        // console.log("RESULTS WITH CONNECTIONS: ", resultsWithConnections);

        // Шаг 5: Сгруппировать объединённые результаты с связями
        const updatedJuridical = resultsWithConnections.filter(item => item.type === 'juridical');
        const updatedIp = resultsWithConnections.filter(item => item.type === 'ip');
        const updatedPhysical = resultsWithConnections.filter(item => item.type === 'physical');

        // Отправить ответ
        res.json({
            juridical: updatedJuridical,
            physical: updatedPhysical,
            ip: updatedIp,
            delta_results: deltaResults
        });

    } catch (error) {
        console.error('Неожиданная ошибка в /api/search:', error);
        res.status(500).json({ error: 'Внутренняя ошибка сервера' });
    }
});

app.listen(3000, () => {
    console.log('✅ Сервер запущен: http://localhost:3000');
});