import { BrowserRouter as Router, Routes, Route } from "react-router-dom";
import DataBrowser from "@/pages/DataBrowser";
import Workspace from "@/pages/Workspace";

export default function App() {
  return (
    <Router>
      <Routes>
        <Route path="/" element={<DataBrowser />} />
        <Route path="/workspace" element={<Workspace />} />
      </Routes>
    </Router>
  );
}
