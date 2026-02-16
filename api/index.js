process.env.RUNTIME_MODE = "serverless";

const { app, bootstrap } = require("../server");

module.exports = async (req, res) => {
  try {
    await bootstrap({
      startQueue: false,
      startServer: false,
    });
    return app(req, res);
  } catch (error) {
    console.error("Serverless bootstrap failed:", error);
    res.statusCode = 500;
    res.setHeader("content-type", "application/json; charset=utf-8");
    res.end(
      JSON.stringify({
        error: "bootstrap_failed",
        message: "Server initialization failed",
      })
    );
  }
};
