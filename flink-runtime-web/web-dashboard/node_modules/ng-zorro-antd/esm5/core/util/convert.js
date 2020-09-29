/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import * as tslib_1 from "tslib";
import { coerceBooleanProperty, coerceCssPixelValue, _isNumberValue } from '@angular/cdk/coercion';
/**
 * @param {?} value
 * @return {?}
 */
export function toBoolean(value) {
    return coerceBooleanProperty(value);
}
/**
 * @param {?} value
 * @param {?=} fallbackValue
 * @return {?}
 */
export function toNumber(value, fallbackValue) {
    if (fallbackValue === void 0) { fallbackValue = 0; }
    return _isNumberValue(value) ? Number(value) : fallbackValue;
}
/**
 * @param {?} value
 * @return {?}
 */
export function toCssPixel(value) {
    return coerceCssPixelValue(value);
}
// Get the function-property type's value
/**
 * @template T
 * @param {?} prop
 * @param {...?} args
 * @return {?}
 */
export function valueFunctionProp(prop) {
    var args = [];
    for (var _i = 1; _i < arguments.length; _i++) {
        args[_i - 1] = arguments[_i];
    }
    return typeof prop === 'function' ? prop.apply(void 0, tslib_1.__spread(args)) : prop;
}
// tslint:disable-next-line: no-any
/**
 * @template T, D
 * @param {?} name
 * @param {?} fallback
 * @return {?}
 */
function propDecoratorFactory(name, fallback) {
    // tslint:disable-next-line: no-any
    /**
     * @param {?} target
     * @param {?} propName
     * @return {?}
     */
    function propDecorator(target, propName) {
        /** @type {?} */
        var privatePropName = "$$__" + propName;
        if (Object.prototype.hasOwnProperty.call(target, privatePropName)) {
            console.warn("The prop \"" + privatePropName + "\" is already exist, it will be overrided by " + name + " decorator.");
        }
        Object.defineProperty(target, privatePropName, {
            configurable: true,
            writable: true
        });
        Object.defineProperty(target, propName, {
            get: /**
             * @return {?}
             */
            function () {
                return this[privatePropName]; // tslint:disable-line:no-invalid-this
            },
            set: /**
             * @param {?} value
             * @return {?}
             */
            function (value) {
                this[privatePropName] = fallback(value); // tslint:disable-line:no-invalid-this
            }
        });
    }
    return propDecorator;
}
/**
 * Input decorator that handle a prop to do get/set automatically with toBoolean
 *
 * Why not using \@InputBoolean alone without \@Input? AOT needs \@Input to be visible
 *
 * \@howToUse
 * ```
 * \@Input() \@InputBoolean() visible: boolean = false;
 *
 * // Act as below:
 * // \@Input()
 * // get visible() { return this.__visibile; }
 * // set visible(value) { this.__visible = value; }
 * // __visible = false;
 * ```
 * @return {?}
 */
export function InputBoolean() {
    return propDecoratorFactory('InputBoolean', toBoolean);
}
/**
 * @return {?}
 */
export function InputCssPixel() {
    return propDecoratorFactory('InputCssPixel', toCssPixel);
}
/**
 * @return {?}
 */
export function InputNumber() {
    return propDecoratorFactory('InputNumber', toNumber);
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiY29udmVydC5qcyIsInNvdXJjZVJvb3QiOiJuZzovL25nLXpvcnJvLWFudGQvIiwic291cmNlcyI6WyJjb3JlL3V0aWwvY29udmVydC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7OztBQUFBLE9BQU8sRUFBRSxxQkFBcUIsRUFBRSxtQkFBbUIsRUFBRSxjQUFjLEVBQUUsTUFBTSx1QkFBdUIsQ0FBQzs7Ozs7QUFHbkcsTUFBTSxVQUFVLFNBQVMsQ0FBQyxLQUF1QjtJQUMvQyxPQUFPLHFCQUFxQixDQUFDLEtBQUssQ0FBQyxDQUFDO0FBQ3RDLENBQUM7Ozs7OztBQUlELE1BQU0sVUFBVSxRQUFRLENBQUMsS0FBc0IsRUFBRSxhQUF5QjtJQUF6Qiw4QkFBQSxFQUFBLGlCQUF5QjtJQUN4RSxPQUFPLGNBQWMsQ0FBQyxLQUFLLENBQUMsQ0FBQyxDQUFDLENBQUMsTUFBTSxDQUFDLEtBQUssQ0FBQyxDQUFDLENBQUMsQ0FBQyxhQUFhLENBQUM7QUFDL0QsQ0FBQzs7Ozs7QUFFRCxNQUFNLFVBQVUsVUFBVSxDQUFDLEtBQXNCO0lBQy9DLE9BQU8sbUJBQW1CLENBQUMsS0FBSyxDQUFDLENBQUM7QUFDcEMsQ0FBQzs7Ozs7Ozs7QUFHRCxNQUFNLFVBQVUsaUJBQWlCLENBQUksSUFBcUI7SUFBRSxjQUFjO1NBQWQsVUFBYyxFQUFkLHFCQUFjLEVBQWQsSUFBYztRQUFkLDZCQUFjOztJQUN4RSxPQUFPLE9BQU8sSUFBSSxLQUFLLFVBQVUsQ0FBQyxDQUFDLENBQUMsSUFBSSxnQ0FBSSxJQUFJLEdBQUUsQ0FBQyxDQUFDLElBQUksQ0FBQztBQUMzRCxDQUFDOzs7Ozs7OztBQUdELFNBQVMsb0JBQW9CLENBQU8sSUFBWSxFQUFFLFFBQXFCOzs7Ozs7O0lBR3JFLFNBQVMsYUFBYSxDQUFDLE1BQVcsRUFBRSxRQUFnQjs7WUFDNUMsZUFBZSxHQUFHLFNBQU8sUUFBVTtRQUV6QyxJQUFJLE1BQU0sQ0FBQyxTQUFTLENBQUMsY0FBYyxDQUFDLElBQUksQ0FBQyxNQUFNLEVBQUUsZUFBZSxDQUFDLEVBQUU7WUFDakUsT0FBTyxDQUFDLElBQUksQ0FBQyxnQkFBYSxlQUFlLHFEQUErQyxJQUFJLGdCQUFhLENBQUMsQ0FBQztTQUM1RztRQUVELE1BQU0sQ0FBQyxjQUFjLENBQUMsTUFBTSxFQUFFLGVBQWUsRUFBRTtZQUM3QyxZQUFZLEVBQUUsSUFBSTtZQUNsQixRQUFRLEVBQU0sSUFBSTtTQUNuQixDQUFDLENBQUM7UUFFSCxNQUFNLENBQUMsY0FBYyxDQUFDLE1BQU0sRUFBRSxRQUFRLEVBQUU7WUFDdEMsR0FBRzs7O1lBQUg7Z0JBQ0UsT0FBTyxJQUFJLENBQUUsZUFBZSxDQUFFLENBQUMsQ0FBQyxzQ0FBc0M7WUFDeEUsQ0FBQztZQUNELEdBQUc7Ozs7WUFBSCxVQUFJLEtBQVE7Z0JBQ1YsSUFBSSxDQUFFLGVBQWUsQ0FBRSxHQUFHLFFBQVEsQ0FBQyxLQUFLLENBQUMsQ0FBQyxDQUFDLHNDQUFzQztZQUNuRixDQUFDO1NBQ0YsQ0FBQyxDQUFDO0lBQ0wsQ0FBQztJQUVELE9BQU8sYUFBYSxDQUFDO0FBRXZCLENBQUM7Ozs7Ozs7Ozs7Ozs7Ozs7OztBQWtCRCxNQUFNLFVBQVUsWUFBWTtJQUMxQixPQUFPLG9CQUFvQixDQUFDLGNBQWMsRUFBRSxTQUFTLENBQUMsQ0FBQztBQUN6RCxDQUFDOzs7O0FBRUQsTUFBTSxVQUFVLGFBQWE7SUFDM0IsT0FBTyxvQkFBb0IsQ0FBQyxlQUFlLEVBQUUsVUFBVSxDQUFDLENBQUM7QUFDM0QsQ0FBQzs7OztBQUVELE1BQU0sVUFBVSxXQUFXO0lBQ3pCLE9BQU8sb0JBQW9CLENBQUMsYUFBYSxFQUFFLFFBQVEsQ0FBQyxDQUFDO0FBQ3ZELENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyJpbXBvcnQgeyBjb2VyY2VCb29sZWFuUHJvcGVydHksIGNvZXJjZUNzc1BpeGVsVmFsdWUsIF9pc051bWJlclZhbHVlIH0gZnJvbSAnQGFuZ3VsYXIvY2RrL2NvZXJjaW9uJztcbmltcG9ydCB7IEZ1bmN0aW9uUHJvcCB9IGZyb20gJy4uL3R5cGVzL2NvbW1vbi13cmFwJztcblxuZXhwb3J0IGZ1bmN0aW9uIHRvQm9vbGVhbih2YWx1ZTogYm9vbGVhbiB8IHN0cmluZyk6IGJvb2xlYW4ge1xuICByZXR1cm4gY29lcmNlQm9vbGVhblByb3BlcnR5KHZhbHVlKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHRvTnVtYmVyKHZhbHVlOiBudW1iZXIgfCBzdHJpbmcpOiBudW1iZXI7XG5leHBvcnQgZnVuY3Rpb24gdG9OdW1iZXI8RD4odmFsdWU6IG51bWJlciB8IHN0cmluZywgZmFsbGJhY2s6IEQpOiBudW1iZXIgfCBEO1xuZXhwb3J0IGZ1bmN0aW9uIHRvTnVtYmVyKHZhbHVlOiBudW1iZXIgfCBzdHJpbmcsIGZhbGxiYWNrVmFsdWU6IG51bWJlciA9IDApOiBudW1iZXIge1xuICByZXR1cm4gX2lzTnVtYmVyVmFsdWUodmFsdWUpID8gTnVtYmVyKHZhbHVlKSA6IGZhbGxiYWNrVmFsdWU7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiB0b0Nzc1BpeGVsKHZhbHVlOiBudW1iZXIgfCBzdHJpbmcpOiBzdHJpbmcge1xuICByZXR1cm4gY29lcmNlQ3NzUGl4ZWxWYWx1ZSh2YWx1ZSk7XG59XG5cbi8vIEdldCB0aGUgZnVuY3Rpb24tcHJvcGVydHkgdHlwZSdzIHZhbHVlXG5leHBvcnQgZnVuY3Rpb24gdmFsdWVGdW5jdGlvblByb3A8VD4ocHJvcDogRnVuY3Rpb25Qcm9wPFQ+LCAuLi5hcmdzOiBhbnlbXSk6IFQgeyAvLyB0c2xpbnQ6ZGlzYWJsZS1saW5lOiBuby1hbnlcbiAgcmV0dXJuIHR5cGVvZiBwcm9wID09PSAnZnVuY3Rpb24nID8gcHJvcCguLi5hcmdzKSA6IHByb3A7XG59XG5cbi8vIHRzbGludDpkaXNhYmxlLW5leHQtbGluZTogbm8tYW55XG5mdW5jdGlvbiBwcm9wRGVjb3JhdG9yRmFjdG9yeTxULCBEPihuYW1lOiBzdHJpbmcsIGZhbGxiYWNrOiAodjogVCkgPT4gRCk6ICh0YXJnZXQ6IGFueSwgcHJvcE5hbWU6IHN0cmluZykgPT4gdm9pZCB7XG5cbiAgLy8gdHNsaW50OmRpc2FibGUtbmV4dC1saW5lOiBuby1hbnlcbiAgZnVuY3Rpb24gcHJvcERlY29yYXRvcih0YXJnZXQ6IGFueSwgcHJvcE5hbWU6IHN0cmluZyk6IHZvaWQge1xuICAgIGNvbnN0IHByaXZhdGVQcm9wTmFtZSA9IGAkJF9fJHtwcm9wTmFtZX1gO1xuXG4gICAgaWYgKE9iamVjdC5wcm90b3R5cGUuaGFzT3duUHJvcGVydHkuY2FsbCh0YXJnZXQsIHByaXZhdGVQcm9wTmFtZSkpIHtcbiAgICAgIGNvbnNvbGUud2FybihgVGhlIHByb3AgXCIke3ByaXZhdGVQcm9wTmFtZX1cIiBpcyBhbHJlYWR5IGV4aXN0LCBpdCB3aWxsIGJlIG92ZXJyaWRlZCBieSAke25hbWV9IGRlY29yYXRvci5gKTtcbiAgICB9XG5cbiAgICBPYmplY3QuZGVmaW5lUHJvcGVydHkodGFyZ2V0LCBwcml2YXRlUHJvcE5hbWUsIHtcbiAgICAgIGNvbmZpZ3VyYWJsZTogdHJ1ZSxcbiAgICAgIHdyaXRhYmxlICAgIDogdHJ1ZVxuICAgIH0pO1xuXG4gICAgT2JqZWN0LmRlZmluZVByb3BlcnR5KHRhcmdldCwgcHJvcE5hbWUsIHtcbiAgICAgIGdldCgpOiBzdHJpbmcge1xuICAgICAgICByZXR1cm4gdGhpc1sgcHJpdmF0ZVByb3BOYW1lIF07IC8vIHRzbGludDpkaXNhYmxlLWxpbmU6bm8taW52YWxpZC10aGlzXG4gICAgICB9LFxuICAgICAgc2V0KHZhbHVlOiBUKTogdm9pZCB7XG4gICAgICAgIHRoaXNbIHByaXZhdGVQcm9wTmFtZSBdID0gZmFsbGJhY2sodmFsdWUpOyAvLyB0c2xpbnQ6ZGlzYWJsZS1saW5lOm5vLWludmFsaWQtdGhpc1xuICAgICAgfVxuICAgIH0pO1xuICB9XG5cbiAgcmV0dXJuIHByb3BEZWNvcmF0b3I7XG5cbn1cblxuLyoqXG4gKiBJbnB1dCBkZWNvcmF0b3IgdGhhdCBoYW5kbGUgYSBwcm9wIHRvIGRvIGdldC9zZXQgYXV0b21hdGljYWxseSB3aXRoIHRvQm9vbGVhblxuICpcbiAqIFdoeSBub3QgdXNpbmcgQElucHV0Qm9vbGVhbiBhbG9uZSB3aXRob3V0IEBJbnB1dD8gQU9UIG5lZWRzIEBJbnB1dCB0byBiZSB2aXNpYmxlXG4gKlxuICogQGhvd1RvVXNlXG4gKiBgYGBcbiAqIEBJbnB1dCgpIEBJbnB1dEJvb2xlYW4oKSB2aXNpYmxlOiBib29sZWFuID0gZmFsc2U7XG4gKlxuICogLy8gQWN0IGFzIGJlbG93OlxuICogLy8gQElucHV0KClcbiAqIC8vIGdldCB2aXNpYmxlKCkgeyByZXR1cm4gdGhpcy5fX3Zpc2liaWxlOyB9XG4gKiAvLyBzZXQgdmlzaWJsZSh2YWx1ZSkgeyB0aGlzLl9fdmlzaWJsZSA9IHZhbHVlOyB9XG4gKiAvLyBfX3Zpc2libGUgPSBmYWxzZTtcbiAqIGBgYFxuICovXG5leHBvcnQgZnVuY3Rpb24gSW5wdXRCb29sZWFuKCk6IGFueSB7IC8vIHRzbGludDpkaXNhYmxlLWxpbmU6IG5vLWFueVxuICByZXR1cm4gcHJvcERlY29yYXRvckZhY3RvcnkoJ0lucHV0Qm9vbGVhbicsIHRvQm9vbGVhbik7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBJbnB1dENzc1BpeGVsKCk6IGFueSB7IC8vIHRzbGludDpkaXNhYmxlLWxpbmU6IG5vLWFueVxuICByZXR1cm4gcHJvcERlY29yYXRvckZhY3RvcnkoJ0lucHV0Q3NzUGl4ZWwnLCB0b0Nzc1BpeGVsKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIElucHV0TnVtYmVyKCk6IGFueSB7IC8vIHRzbGludDpkaXNhYmxlLWxpbmU6IG5vLWFueVxuICByZXR1cm4gcHJvcERlY29yYXRvckZhY3RvcnkoJ0lucHV0TnVtYmVyJywgdG9OdW1iZXIpO1xufVxuIl19