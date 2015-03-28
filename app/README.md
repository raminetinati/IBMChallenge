# SOCIAM Window on the Web - Define Server

This is part of the bigger, Window on the Web/ Macroscope project

Window on the Web (WoW) is a project to provide a set of realtime and retrospective visualisations of activity on the World Wide Web.

Data is fed from our cluster of web crawling and social media feed reading servers, and served through RabbitMQ.

This server, written in Node.js reads the message queue and sends live updates to visualisation clients, and also provides interactive filtering choreographing to the visualisations.

