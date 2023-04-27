import { DOMParser as dom } from "xmldom";
import * as xpath from "xpath-ts";

const xml = "<book><title>Harry Potter</title></book>";
const doc = new dom().parseFromString(xml);
const nodes: any = xpath.select("//title", doc);
console.log(nodes);

console.log(nodes[0].localName + ": " + nodes[0].firstChild.data);
console.log("Node: " + nodes[0].toString());
