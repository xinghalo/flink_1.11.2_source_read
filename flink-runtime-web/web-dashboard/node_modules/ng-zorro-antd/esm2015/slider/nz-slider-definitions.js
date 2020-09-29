/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/**
 * @record
 */
export function MarkObj() { }
if (false) {
    /** @type {?|undefined} */
    MarkObj.prototype.style;
    /** @type {?} */
    MarkObj.prototype.label;
}
export class Marks {
}
/**
 * Processed steps that would be passed to sub components.
 * @record
 */
export function ExtendedMark() { }
if (false) {
    /** @type {?} */
    ExtendedMark.prototype.value;
    /** @type {?} */
    ExtendedMark.prototype.offset;
    /** @type {?} */
    ExtendedMark.prototype.config;
}
/**
 * Marks that would be rendered.
 * @record
 */
export function DisplayedMark() { }
if (false) {
    /** @type {?} */
    DisplayedMark.prototype.active;
    /** @type {?} */
    DisplayedMark.prototype.label;
    /** @type {?|undefined} */
    DisplayedMark.prototype.style;
}
/**
 * Steps that would be rendered.
 * @record
 */
export function DisplayedStep() { }
if (false) {
    /** @type {?} */
    DisplayedStep.prototype.active;
    /** @type {?|undefined} */
    DisplayedStep.prototype.style;
}
/**
 * @record
 */
export function SliderHandler() { }
if (false) {
    /** @type {?} */
    SliderHandler.prototype.offset;
    /** @type {?} */
    SliderHandler.prototype.value;
    /** @type {?} */
    SliderHandler.prototype.active;
}
/**
 * @param {?} value
 * @return {?}
 */
export function isValueARange(value) {
    if (value instanceof Array) {
        return value.length === 2;
    }
    else {
        return false;
    }
}
/**
 * @param {?} config
 * @return {?}
 */
export function isConfigAObject(config) {
    return config instanceof Object;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotc2xpZGVyLWRlZmluaXRpb25zLmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbInNsaWRlci9uei1zbGlkZXItZGVmaW5pdGlvbnMudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7OztBQUVBLDZCQUdDOzs7SUFGQyx3QkFBZTs7SUFDZix3QkFBYzs7QUFHaEIsTUFBTSxPQUFPLEtBQUs7Q0FFakI7Ozs7O0FBS0Qsa0NBSUM7OztJQUhDLDZCQUFjOztJQUNkLDhCQUFlOztJQUNmLDhCQUFhOzs7Ozs7QUFNZixtQ0FJQzs7O0lBSEMsK0JBQWdCOztJQUNoQiw4QkFBYzs7SUFDZCw4QkFBZTs7Ozs7O0FBTWpCLG1DQUdDOzs7SUFGQywrQkFBZ0I7O0lBQ2hCLDhCQUFlOzs7OztBQU9qQixtQ0FJQzs7O0lBSEMsK0JBQXNCOztJQUN0Qiw4QkFBcUI7O0lBQ3JCLCtCQUFnQjs7Ozs7O0FBR2xCLE1BQU0sVUFBVSxhQUFhLENBQUMsS0FBa0I7SUFDOUMsSUFBSSxLQUFLLFlBQVksS0FBSyxFQUFFO1FBQzFCLE9BQU8sS0FBSyxDQUFDLE1BQU0sS0FBSyxDQUFDLENBQUM7S0FDM0I7U0FBTTtRQUNMLE9BQU8sS0FBSyxDQUFDO0tBQ2Q7QUFDSCxDQUFDOzs7OztBQUVELE1BQU0sVUFBVSxlQUFlLENBQUMsTUFBWTtJQUMxQyxPQUFPLE1BQU0sWUFBWSxNQUFNLENBQUM7QUFDbEMsQ0FBQyIsInNvdXJjZXNDb250ZW50IjpbImV4cG9ydCB0eXBlIE1hcmsgPSBzdHJpbmcgfCBNYXJrT2JqO1xuXG5leHBvcnQgaW50ZXJmYWNlIE1hcmtPYmoge1xuICBzdHlsZT86IG9iamVjdDtcbiAgbGFiZWw6IHN0cmluZztcbn1cblxuZXhwb3J0IGNsYXNzIE1hcmtzIHtcbiAgW2tleTogbnVtYmVyXTogTWFyaztcbn1cblxuLyoqXG4gKiBQcm9jZXNzZWQgc3RlcHMgdGhhdCB3b3VsZCBiZSBwYXNzZWQgdG8gc3ViIGNvbXBvbmVudHMuXG4gKi9cbmV4cG9ydCBpbnRlcmZhY2UgRXh0ZW5kZWRNYXJrIHtcbiAgdmFsdWU6IG51bWJlcjtcbiAgb2Zmc2V0OiBudW1iZXI7XG4gIGNvbmZpZzogTWFyaztcbn1cblxuLyoqXG4gKiBNYXJrcyB0aGF0IHdvdWxkIGJlIHJlbmRlcmVkLlxuICovXG5leHBvcnQgaW50ZXJmYWNlIERpc3BsYXllZE1hcmsgZXh0ZW5kcyBFeHRlbmRlZE1hcmsge1xuICBhY3RpdmU6IGJvb2xlYW47XG4gIGxhYmVsOiBzdHJpbmc7XG4gIHN0eWxlPzogb2JqZWN0O1xufVxuXG4vKipcbiAqIFN0ZXBzIHRoYXQgd291bGQgYmUgcmVuZGVyZWQuXG4gKi9cbmV4cG9ydCBpbnRlcmZhY2UgRGlzcGxheWVkU3RlcCBleHRlbmRzIEV4dGVuZGVkTWFyayB7XG4gIGFjdGl2ZTogYm9vbGVhbjtcbiAgc3R5bGU/OiBvYmplY3Q7XG59XG5cbmV4cG9ydCB0eXBlIFNsaWRlclNob3dUb29sdGlwID0gJ2Fsd2F5cycgfCAnbmV2ZXInIHwgJ2RlZmF1bHQnO1xuXG5leHBvcnQgdHlwZSBTbGlkZXJWYWx1ZSA9IG51bWJlcltdIHwgbnVtYmVyO1xuXG5leHBvcnQgaW50ZXJmYWNlIFNsaWRlckhhbmRsZXIge1xuICBvZmZzZXQ6IG51bWJlciB8IG51bGw7XG4gIHZhbHVlOiBudW1iZXIgfCBudWxsO1xuICBhY3RpdmU6IGJvb2xlYW47XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBpc1ZhbHVlQVJhbmdlKHZhbHVlOiBTbGlkZXJWYWx1ZSk6IHZhbHVlIGlzIG51bWJlcltdIHtcbiAgaWYgKHZhbHVlIGluc3RhbmNlb2YgQXJyYXkpIHtcbiAgICByZXR1cm4gdmFsdWUubGVuZ3RoID09PSAyO1xuICB9IGVsc2Uge1xuICAgIHJldHVybiBmYWxzZTtcbiAgfVxufVxuXG5leHBvcnQgZnVuY3Rpb24gaXNDb25maWdBT2JqZWN0KGNvbmZpZzogTWFyayk6IGNvbmZpZyBpcyBNYXJrT2JqIHtcbiAgcmV0dXJuIGNvbmZpZyBpbnN0YW5jZW9mIE9iamVjdDtcbn1cbiJdfQ==