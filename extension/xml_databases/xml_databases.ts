import { BaseXExtension } from "../basex/basex_extension";

const xml_databases = [
  { name: "basex", driver: new BaseXExtension() },
  { name: "existdb", driver: new BaseXExtension() },
];
export { xml_databases };
