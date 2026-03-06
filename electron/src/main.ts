import { app, BrowserWindow } from "electron";
import path from "node:path";
import url from "node:url";

import { registerIpcHandlers } from "./ipc";

const isDev = !app.isPackaged;

function createWindow(): BrowserWindow {
  const preloadPath = path.join(url.fileURLToPath(new URL(".", import.meta.url)), "preload.js");

  const win = new BrowserWindow({
    width: 1200,
    height: 800,
    backgroundColor: "#0B1020",
    webPreferences: {
      preload: preloadPath,
      contextIsolation: true,
      nodeIntegration: false,
    },
  });

  if (isDev) {
    win.loadURL("http://localhost:5173/");
    win.webContents.openDevTools({ mode: "detach" });
  } else {
    const indexPath = path.join(url.fileURLToPath(new URL(".", import.meta.url)), "..", "..", "dist", "index.html");
    win.loadFile(indexPath);
  }

  return win;
}

app.whenReady().then(() => {
  registerIpcHandlers();
  createWindow();

  app.on("activate", () => {
    if (BrowserWindow.getAllWindows().length === 0) {
      createWindow();
    }
  });
});

app.on("window-all-closed", () => {
  if (process.platform !== "darwin") {
    app.quit();
  }
});

