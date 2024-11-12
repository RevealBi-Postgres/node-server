const express = require('express');
const cors = require('cors');
const path = require("path");
const swaggerUi = require('swagger-ui-express');
const swaggerJSDoc = require('swagger-jsdoc');

const reveal = require('./reveal');  
const revealDom = require('./revealdom');

const app = express();
const port = 5111;

// Middleware
app.use(cors()); 
app.use("/images/png", express.static("images/png"));
app.use("/images/svg", express.static("images/svg"));
app.use(express.static(path.join(__dirname)));

// Swagger setup
const swaggerOptions = {
    definition: {
        openapi: '3.0.0',
        info: {
            title: 'Reveal BI API',
            version: '1.0.0',
            description: 'API for interacting with Reveal BI dashboards',
        },
    },
    apis: ['./revealdom.js'], 
};

const swaggerDocs = swaggerJSDoc(swaggerOptions);
app.use('/api-docs', swaggerUi.serve, swaggerUi.setup(swaggerDocs));
app.get('/swagger.json', (req, res) => {
    res.setHeader('Content-Type', 'application/json');
    res.send(swaggerDocs);
});

// Use the routes from revealDom.js
revealDom(app);

// Use the Reveal SDK middleware
app.use("/", reveal);  

// Start the server
app.listen(port, () => {
    console.log(`Reveal server accepting http requests on port ${port}`);
});
