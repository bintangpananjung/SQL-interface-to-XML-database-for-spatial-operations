interface x {
  a: string;
}

interface y extends x {
  b: number;
}

const z: y = { a: "", b: 10 };
