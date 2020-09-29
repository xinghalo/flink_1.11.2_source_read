/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
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
export function toNumber(value, fallbackValue = 0) {
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
export function valueFunctionProp(prop, ...args) {
    return typeof prop === 'function' ? prop(...args) : prop;
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
        const privatePropName = `$$__${propName}`;
        if (Object.prototype.hasOwnProperty.call(target, privatePropName)) {
            console.warn(`The prop "${privatePropName}" is already exist, it will be overrided by ${name} decorator.`);
        }
        Object.defineProperty(target, privatePropName, {
            configurable: true,
            writable: true
        });
        Object.defineProperty(target, propName, {
            /**
             * @return {?}
             */
            get() {
                return this[privatePropName]; // tslint:disable-line:no-invalid-this
            },
            /**
             * @param {?} value
             * @return {?}
             */
            set(value) {
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
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiY29udmVydC5qcyIsInNvdXJjZVJvb3QiOiJuZzovL25nLXpvcnJvLWFudGQvIiwic291cmNlcyI6WyJjb3JlL3V0aWwvY29udmVydC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7O0FBQUEsT0FBTyxFQUFFLHFCQUFxQixFQUFFLG1CQUFtQixFQUFFLGNBQWMsRUFBRSxNQUFNLHVCQUF1QixDQUFDOzs7OztBQUduRyxNQUFNLFVBQVUsU0FBUyxDQUFDLEtBQXVCO0lBQy9DLE9BQU8scUJBQXFCLENBQUMsS0FBSyxDQUFDLENBQUM7QUFDdEMsQ0FBQzs7Ozs7O0FBSUQsTUFBTSxVQUFVLFFBQVEsQ0FBQyxLQUFzQixFQUFFLGdCQUF3QixDQUFDO0lBQ3hFLE9BQU8sY0FBYyxDQUFDLEtBQUssQ0FBQyxDQUFDLENBQUMsQ0FBQyxNQUFNLENBQUMsS0FBSyxDQUFDLENBQUMsQ0FBQyxDQUFDLGFBQWEsQ0FBQztBQUMvRCxDQUFDOzs7OztBQUVELE1BQU0sVUFBVSxVQUFVLENBQUMsS0FBc0I7SUFDL0MsT0FBTyxtQkFBbUIsQ0FBQyxLQUFLLENBQUMsQ0FBQztBQUNwQyxDQUFDOzs7Ozs7OztBQUdELE1BQU0sVUFBVSxpQkFBaUIsQ0FBSSxJQUFxQixFQUFFLEdBQUcsSUFBVztJQUN4RSxPQUFPLE9BQU8sSUFBSSxLQUFLLFVBQVUsQ0FBQyxDQUFDLENBQUMsSUFBSSxDQUFDLEdBQUcsSUFBSSxDQUFDLENBQUMsQ0FBQyxDQUFDLElBQUksQ0FBQztBQUMzRCxDQUFDOzs7Ozs7OztBQUdELFNBQVMsb0JBQW9CLENBQU8sSUFBWSxFQUFFLFFBQXFCOzs7Ozs7O0lBR3JFLFNBQVMsYUFBYSxDQUFDLE1BQVcsRUFBRSxRQUFnQjs7Y0FDNUMsZUFBZSxHQUFHLE9BQU8sUUFBUSxFQUFFO1FBRXpDLElBQUksTUFBTSxDQUFDLFNBQVMsQ0FBQyxjQUFjLENBQUMsSUFBSSxDQUFDLE1BQU0sRUFBRSxlQUFlLENBQUMsRUFBRTtZQUNqRSxPQUFPLENBQUMsSUFBSSxDQUFDLGFBQWEsZUFBZSwrQ0FBK0MsSUFBSSxhQUFhLENBQUMsQ0FBQztTQUM1RztRQUVELE1BQU0sQ0FBQyxjQUFjLENBQUMsTUFBTSxFQUFFLGVBQWUsRUFBRTtZQUM3QyxZQUFZLEVBQUUsSUFBSTtZQUNsQixRQUFRLEVBQU0sSUFBSTtTQUNuQixDQUFDLENBQUM7UUFFSCxNQUFNLENBQUMsY0FBYyxDQUFDLE1BQU0sRUFBRSxRQUFRLEVBQUU7Ozs7WUFDdEMsR0FBRztnQkFDRCxPQUFPLElBQUksQ0FBRSxlQUFlLENBQUUsQ0FBQyxDQUFDLHNDQUFzQztZQUN4RSxDQUFDOzs7OztZQUNELEdBQUcsQ0FBQyxLQUFRO2dCQUNWLElBQUksQ0FBRSxlQUFlLENBQUUsR0FBRyxRQUFRLENBQUMsS0FBSyxDQUFDLENBQUMsQ0FBQyxzQ0FBc0M7WUFDbkYsQ0FBQztTQUNGLENBQUMsQ0FBQztJQUNMLENBQUM7SUFFRCxPQUFPLGFBQWEsQ0FBQztBQUV2QixDQUFDOzs7Ozs7Ozs7Ozs7Ozs7Ozs7QUFrQkQsTUFBTSxVQUFVLFlBQVk7SUFDMUIsT0FBTyxvQkFBb0IsQ0FBQyxjQUFjLEVBQUUsU0FBUyxDQUFDLENBQUM7QUFDekQsQ0FBQzs7OztBQUVELE1BQU0sVUFBVSxhQUFhO0lBQzNCLE9BQU8sb0JBQW9CLENBQUMsZUFBZSxFQUFFLFVBQVUsQ0FBQyxDQUFDO0FBQzNELENBQUM7Ozs7QUFFRCxNQUFNLFVBQVUsV0FBVztJQUN6QixPQUFPLG9CQUFvQixDQUFDLGFBQWEsRUFBRSxRQUFRLENBQUMsQ0FBQztBQUN2RCxDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHsgY29lcmNlQm9vbGVhblByb3BlcnR5LCBjb2VyY2VDc3NQaXhlbFZhbHVlLCBfaXNOdW1iZXJWYWx1ZSB9IGZyb20gJ0Bhbmd1bGFyL2Nkay9jb2VyY2lvbic7XG5pbXBvcnQgeyBGdW5jdGlvblByb3AgfSBmcm9tICcuLi90eXBlcy9jb21tb24td3JhcCc7XG5cbmV4cG9ydCBmdW5jdGlvbiB0b0Jvb2xlYW4odmFsdWU6IGJvb2xlYW4gfCBzdHJpbmcpOiBib29sZWFuIHtcbiAgcmV0dXJuIGNvZXJjZUJvb2xlYW5Qcm9wZXJ0eSh2YWx1ZSk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiB0b051bWJlcih2YWx1ZTogbnVtYmVyIHwgc3RyaW5nKTogbnVtYmVyO1xuZXhwb3J0IGZ1bmN0aW9uIHRvTnVtYmVyPEQ+KHZhbHVlOiBudW1iZXIgfCBzdHJpbmcsIGZhbGxiYWNrOiBEKTogbnVtYmVyIHwgRDtcbmV4cG9ydCBmdW5jdGlvbiB0b051bWJlcih2YWx1ZTogbnVtYmVyIHwgc3RyaW5nLCBmYWxsYmFja1ZhbHVlOiBudW1iZXIgPSAwKTogbnVtYmVyIHtcbiAgcmV0dXJuIF9pc051bWJlclZhbHVlKHZhbHVlKSA/IE51bWJlcih2YWx1ZSkgOiBmYWxsYmFja1ZhbHVlO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gdG9Dc3NQaXhlbCh2YWx1ZTogbnVtYmVyIHwgc3RyaW5nKTogc3RyaW5nIHtcbiAgcmV0dXJuIGNvZXJjZUNzc1BpeGVsVmFsdWUodmFsdWUpO1xufVxuXG4vLyBHZXQgdGhlIGZ1bmN0aW9uLXByb3BlcnR5IHR5cGUncyB2YWx1ZVxuZXhwb3J0IGZ1bmN0aW9uIHZhbHVlRnVuY3Rpb25Qcm9wPFQ+KHByb3A6IEZ1bmN0aW9uUHJvcDxUPiwgLi4uYXJnczogYW55W10pOiBUIHsgLy8gdHNsaW50OmRpc2FibGUtbGluZTogbm8tYW55XG4gIHJldHVybiB0eXBlb2YgcHJvcCA9PT0gJ2Z1bmN0aW9uJyA/IHByb3AoLi4uYXJncykgOiBwcm9wO1xufVxuXG4vLyB0c2xpbnQ6ZGlzYWJsZS1uZXh0LWxpbmU6IG5vLWFueVxuZnVuY3Rpb24gcHJvcERlY29yYXRvckZhY3Rvcnk8VCwgRD4obmFtZTogc3RyaW5nLCBmYWxsYmFjazogKHY6IFQpID0+IEQpOiAodGFyZ2V0OiBhbnksIHByb3BOYW1lOiBzdHJpbmcpID0+IHZvaWQge1xuXG4gIC8vIHRzbGludDpkaXNhYmxlLW5leHQtbGluZTogbm8tYW55XG4gIGZ1bmN0aW9uIHByb3BEZWNvcmF0b3IodGFyZ2V0OiBhbnksIHByb3BOYW1lOiBzdHJpbmcpOiB2b2lkIHtcbiAgICBjb25zdCBwcml2YXRlUHJvcE5hbWUgPSBgJCRfXyR7cHJvcE5hbWV9YDtcblxuICAgIGlmIChPYmplY3QucHJvdG90eXBlLmhhc093blByb3BlcnR5LmNhbGwodGFyZ2V0LCBwcml2YXRlUHJvcE5hbWUpKSB7XG4gICAgICBjb25zb2xlLndhcm4oYFRoZSBwcm9wIFwiJHtwcml2YXRlUHJvcE5hbWV9XCIgaXMgYWxyZWFkeSBleGlzdCwgaXQgd2lsbCBiZSBvdmVycmlkZWQgYnkgJHtuYW1lfSBkZWNvcmF0b3IuYCk7XG4gICAgfVxuXG4gICAgT2JqZWN0LmRlZmluZVByb3BlcnR5KHRhcmdldCwgcHJpdmF0ZVByb3BOYW1lLCB7XG4gICAgICBjb25maWd1cmFibGU6IHRydWUsXG4gICAgICB3cml0YWJsZSAgICA6IHRydWVcbiAgICB9KTtcblxuICAgIE9iamVjdC5kZWZpbmVQcm9wZXJ0eSh0YXJnZXQsIHByb3BOYW1lLCB7XG4gICAgICBnZXQoKTogc3RyaW5nIHtcbiAgICAgICAgcmV0dXJuIHRoaXNbIHByaXZhdGVQcm9wTmFtZSBdOyAvLyB0c2xpbnQ6ZGlzYWJsZS1saW5lOm5vLWludmFsaWQtdGhpc1xuICAgICAgfSxcbiAgICAgIHNldCh2YWx1ZTogVCk6IHZvaWQge1xuICAgICAgICB0aGlzWyBwcml2YXRlUHJvcE5hbWUgXSA9IGZhbGxiYWNrKHZhbHVlKTsgLy8gdHNsaW50OmRpc2FibGUtbGluZTpuby1pbnZhbGlkLXRoaXNcbiAgICAgIH1cbiAgICB9KTtcbiAgfVxuXG4gIHJldHVybiBwcm9wRGVjb3JhdG9yO1xuXG59XG5cbi8qKlxuICogSW5wdXQgZGVjb3JhdG9yIHRoYXQgaGFuZGxlIGEgcHJvcCB0byBkbyBnZXQvc2V0IGF1dG9tYXRpY2FsbHkgd2l0aCB0b0Jvb2xlYW5cbiAqXG4gKiBXaHkgbm90IHVzaW5nIEBJbnB1dEJvb2xlYW4gYWxvbmUgd2l0aG91dCBASW5wdXQ/IEFPVCBuZWVkcyBASW5wdXQgdG8gYmUgdmlzaWJsZVxuICpcbiAqIEBob3dUb1VzZVxuICogYGBgXG4gKiBASW5wdXQoKSBASW5wdXRCb29sZWFuKCkgdmlzaWJsZTogYm9vbGVhbiA9IGZhbHNlO1xuICpcbiAqIC8vIEFjdCBhcyBiZWxvdzpcbiAqIC8vIEBJbnB1dCgpXG4gKiAvLyBnZXQgdmlzaWJsZSgpIHsgcmV0dXJuIHRoaXMuX192aXNpYmlsZTsgfVxuICogLy8gc2V0IHZpc2libGUodmFsdWUpIHsgdGhpcy5fX3Zpc2libGUgPSB2YWx1ZTsgfVxuICogLy8gX192aXNpYmxlID0gZmFsc2U7XG4gKiBgYGBcbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIElucHV0Qm9vbGVhbigpOiBhbnkgeyAvLyB0c2xpbnQ6ZGlzYWJsZS1saW5lOiBuby1hbnlcbiAgcmV0dXJuIHByb3BEZWNvcmF0b3JGYWN0b3J5KCdJbnB1dEJvb2xlYW4nLCB0b0Jvb2xlYW4pO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gSW5wdXRDc3NQaXhlbCgpOiBhbnkgeyAvLyB0c2xpbnQ6ZGlzYWJsZS1saW5lOiBuby1hbnlcbiAgcmV0dXJuIHByb3BEZWNvcmF0b3JGYWN0b3J5KCdJbnB1dENzc1BpeGVsJywgdG9Dc3NQaXhlbCk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBJbnB1dE51bWJlcigpOiBhbnkgeyAvLyB0c2xpbnQ6ZGlzYWJsZS1saW5lOiBuby1hbnlcbiAgcmV0dXJuIHByb3BEZWNvcmF0b3JGYWN0b3J5KCdJbnB1dE51bWJlcicsIHRvTnVtYmVyKTtcbn1cbiJdfQ==