import { FunctionProp } from '../types/common-wrap';
export declare function toBoolean(value: boolean | string): boolean;
export declare function toNumber(value: number | string): number;
export declare function toNumber<D>(value: number | string, fallback: D): number | D;
export declare function toCssPixel(value: number | string): string;
export declare function valueFunctionProp<T>(prop: FunctionProp<T>, ...args: any[]): T;
/**
 * Input decorator that handle a prop to do get/set automatically with toBoolean
 *
 * Why not using @InputBoolean alone without @Input? AOT needs @Input to be visible
 *
 * @howToUse
 * ```
 * @Input() @InputBoolean() visible: boolean = false;
 *
 * // Act as below:
 * // @Input()
 * // get visible() { return this.__visibile; }
 * // set visible(value) { this.__visible = value; }
 * // __visible = false;
 * ```
 */
export declare function InputBoolean(): any;
export declare function InputCssPixel(): any;
export declare function InputNumber(): any;
