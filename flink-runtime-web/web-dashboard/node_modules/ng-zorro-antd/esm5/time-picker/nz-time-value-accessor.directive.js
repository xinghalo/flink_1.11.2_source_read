/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { Directive, ElementRef, HostListener, Input } from '@angular/core';
import { NG_VALUE_ACCESSOR } from '@angular/forms';
import { DateHelperService } from '../i18n/date-helper.service';
var NzTimeValueAccessorDirective = /** @class */ (function () {
    function NzTimeValueAccessorDirective(dateHelper, elementRef) {
        this.dateHelper = dateHelper;
        this.elementRef = elementRef;
    }
    /**
     * @return {?}
     */
    NzTimeValueAccessorDirective.prototype.keyup = /**
     * @return {?}
     */
    function () {
        this.changed();
    };
    /**
     * @return {?}
     */
    NzTimeValueAccessorDirective.prototype.blur = /**
     * @return {?}
     */
    function () {
        this.touched();
    };
    /**
     * @return {?}
     */
    NzTimeValueAccessorDirective.prototype.changed = /**
     * @return {?}
     */
    function () {
        if (this._onChange) {
            /** @type {?} */
            var value = this.dateHelper.parseTime(this.elementRef.nativeElement.value);
            this._onChange((/** @type {?} */ (value)));
        }
    };
    /**
     * @return {?}
     */
    NzTimeValueAccessorDirective.prototype.touched = /**
     * @return {?}
     */
    function () {
        if (this._onTouch) {
            this._onTouch();
        }
    };
    /**
     * @return {?}
     */
    NzTimeValueAccessorDirective.prototype.setRange = /**
     * @return {?}
     */
    function () {
        this.elementRef.nativeElement.focus();
        this.elementRef.nativeElement.setSelectionRange(0, this.elementRef.nativeElement.value.length);
    };
    /**
     * @param {?} value
     * @return {?}
     */
    NzTimeValueAccessorDirective.prototype.writeValue = /**
     * @param {?} value
     * @return {?}
     */
    function (value) {
        this.elementRef.nativeElement.value = this.dateHelper.format(value, this.nzTime);
    };
    /**
     * @param {?} fn
     * @return {?}
     */
    NzTimeValueAccessorDirective.prototype.registerOnChange = /**
     * @param {?} fn
     * @return {?}
     */
    function (fn) {
        this._onChange = fn;
    };
    /**
     * @param {?} fn
     * @return {?}
     */
    NzTimeValueAccessorDirective.prototype.registerOnTouched = /**
     * @param {?} fn
     * @return {?}
     */
    function (fn) {
        this._onTouch = fn;
    };
    NzTimeValueAccessorDirective.decorators = [
        { type: Directive, args: [{
                    selector: 'input[nzTime]',
                    providers: [{ provide: NG_VALUE_ACCESSOR, useExisting: NzTimeValueAccessorDirective, multi: true }]
                },] }
    ];
    /** @nocollapse */
    NzTimeValueAccessorDirective.ctorParameters = function () { return [
        { type: DateHelperService },
        { type: ElementRef }
    ]; };
    NzTimeValueAccessorDirective.propDecorators = {
        nzTime: [{ type: Input }],
        keyup: [{ type: HostListener, args: ['keyup',] }],
        blur: [{ type: HostListener, args: ['blur',] }]
    };
    return NzTimeValueAccessorDirective;
}());
export { NzTimeValueAccessorDirective };
if (false) {
    /**
     * @type {?}
     * @private
     */
    NzTimeValueAccessorDirective.prototype._onChange;
    /**
     * @type {?}
     * @private
     */
    NzTimeValueAccessorDirective.prototype._onTouch;
    /** @type {?} */
    NzTimeValueAccessorDirective.prototype.nzTime;
    /**
     * @type {?}
     * @private
     */
    NzTimeValueAccessorDirective.prototype.dateHelper;
    /**
     * @type {?}
     * @private
     */
    NzTimeValueAccessorDirective.prototype.elementRef;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotdGltZS12YWx1ZS1hY2Nlc3Nvci5kaXJlY3RpdmUuanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsidGltZS1waWNrZXIvbnotdGltZS12YWx1ZS1hY2Nlc3Nvci5kaXJlY3RpdmUudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7OztBQUFBLE9BQU8sRUFBRSxTQUFTLEVBQUUsVUFBVSxFQUFFLFlBQVksRUFBRSxLQUFLLEVBQUUsTUFBTSxlQUFlLENBQUM7QUFDM0UsT0FBTyxFQUF3QixpQkFBaUIsRUFBRSxNQUFNLGdCQUFnQixDQUFDO0FBQ3pFLE9BQU8sRUFBRSxpQkFBaUIsRUFBRSxNQUFNLDZCQUE2QixDQUFDO0FBRWhFO0lBcUNFLHNDQUFvQixVQUE2QixFQUFVLFVBQXNCO1FBQTdELGVBQVUsR0FBVixVQUFVLENBQW1CO1FBQVUsZUFBVSxHQUFWLFVBQVUsQ0FBWTtJQUFHLENBQUM7Ozs7SUEzQnJGLDRDQUFLOzs7SUFETDtRQUVFLElBQUksQ0FBQyxPQUFPLEVBQUUsQ0FBQztJQUNqQixDQUFDOzs7O0lBR0QsMkNBQUk7OztJQURKO1FBRUUsSUFBSSxDQUFDLE9BQU8sRUFBRSxDQUFDO0lBQ2pCLENBQUM7Ozs7SUFFRCw4Q0FBTzs7O0lBQVA7UUFDRSxJQUFJLElBQUksQ0FBQyxTQUFTLEVBQUU7O2dCQUNaLEtBQUssR0FBRyxJQUFJLENBQUMsVUFBVSxDQUFDLFNBQVMsQ0FBQyxJQUFJLENBQUMsVUFBVSxDQUFDLGFBQWEsQ0FBQyxLQUFLLENBQUM7WUFDNUUsSUFBSSxDQUFDLFNBQVMsQ0FBQyxtQkFBQSxLQUFLLEVBQUMsQ0FBQyxDQUFDO1NBQ3hCO0lBQ0gsQ0FBQzs7OztJQUVELDhDQUFPOzs7SUFBUDtRQUNFLElBQUksSUFBSSxDQUFDLFFBQVEsRUFBRTtZQUNqQixJQUFJLENBQUMsUUFBUSxFQUFFLENBQUM7U0FDakI7SUFDSCxDQUFDOzs7O0lBRUQsK0NBQVE7OztJQUFSO1FBQ0UsSUFBSSxDQUFDLFVBQVUsQ0FBQyxhQUFhLENBQUMsS0FBSyxFQUFFLENBQUM7UUFDdEMsSUFBSSxDQUFDLFVBQVUsQ0FBQyxhQUFhLENBQUMsaUJBQWlCLENBQUMsQ0FBQyxFQUFFLElBQUksQ0FBQyxVQUFVLENBQUMsYUFBYSxDQUFDLEtBQUssQ0FBQyxNQUFNLENBQUMsQ0FBQztJQUNqRyxDQUFDOzs7OztJQUlELGlEQUFVOzs7O0lBQVYsVUFBVyxLQUFXO1FBQ3BCLElBQUksQ0FBQyxVQUFVLENBQUMsYUFBYSxDQUFDLEtBQUssR0FBRyxJQUFJLENBQUMsVUFBVSxDQUFDLE1BQU0sQ0FBQyxLQUFLLEVBQUUsSUFBSSxDQUFDLE1BQU0sQ0FBQyxDQUFDO0lBQ25GLENBQUM7Ozs7O0lBRUQsdURBQWdCOzs7O0lBQWhCLFVBQWlCLEVBQXlCO1FBQ3hDLElBQUksQ0FBQyxTQUFTLEdBQUcsRUFBRSxDQUFDO0lBQ3RCLENBQUM7Ozs7O0lBRUQsd0RBQWlCOzs7O0lBQWpCLFVBQWtCLEVBQWM7UUFDOUIsSUFBSSxDQUFDLFFBQVEsR0FBRyxFQUFFLENBQUM7SUFDckIsQ0FBQzs7Z0JBakRGLFNBQVMsU0FBQztvQkFDVCxRQUFRLEVBQUUsZUFBZTtvQkFDekIsU0FBUyxFQUFFLENBQUMsRUFBRSxPQUFPLEVBQUUsaUJBQWlCLEVBQUUsV0FBVyxFQUFFLDRCQUE0QixFQUFFLEtBQUssRUFBRSxJQUFJLEVBQUUsQ0FBQztpQkFDcEc7Ozs7Z0JBTFEsaUJBQWlCO2dCQUZOLFVBQVU7Ozt5QkFXM0IsS0FBSzt3QkFFTCxZQUFZLFNBQUMsT0FBTzt1QkFLcEIsWUFBWSxTQUFDLE1BQU07O0lBb0N0QixtQ0FBQztDQUFBLEFBbERELElBa0RDO1NBOUNZLDRCQUE0Qjs7Ozs7O0lBQ3ZDLGlEQUF5Qzs7Ozs7SUFDekMsZ0RBQTZCOztJQUM3Qiw4Q0FBd0I7Ozs7O0lBOEJaLGtEQUFxQzs7Ozs7SUFBRSxrREFBOEIiLCJzb3VyY2VzQ29udGVudCI6WyJpbXBvcnQgeyBEaXJlY3RpdmUsIEVsZW1lbnRSZWYsIEhvc3RMaXN0ZW5lciwgSW5wdXQgfSBmcm9tICdAYW5ndWxhci9jb3JlJztcbmltcG9ydCB7IENvbnRyb2xWYWx1ZUFjY2Vzc29yLCBOR19WQUxVRV9BQ0NFU1NPUiB9IGZyb20gJ0Bhbmd1bGFyL2Zvcm1zJztcbmltcG9ydCB7IERhdGVIZWxwZXJTZXJ2aWNlIH0gZnJvbSAnLi4vaTE4bi9kYXRlLWhlbHBlci5zZXJ2aWNlJztcblxuQERpcmVjdGl2ZSh7XG4gIHNlbGVjdG9yOiAnaW5wdXRbbnpUaW1lXScsXG4gIHByb3ZpZGVyczogW3sgcHJvdmlkZTogTkdfVkFMVUVfQUNDRVNTT1IsIHVzZUV4aXN0aW5nOiBOelRpbWVWYWx1ZUFjY2Vzc29yRGlyZWN0aXZlLCBtdWx0aTogdHJ1ZSB9XVxufSlcbmV4cG9ydCBjbGFzcyBOelRpbWVWYWx1ZUFjY2Vzc29yRGlyZWN0aXZlIGltcGxlbWVudHMgQ29udHJvbFZhbHVlQWNjZXNzb3Ige1xuICBwcml2YXRlIF9vbkNoYW5nZTogKHZhbHVlOiBEYXRlKSA9PiB2b2lkO1xuICBwcml2YXRlIF9vblRvdWNoOiAoKSA9PiB2b2lkO1xuICBASW5wdXQoKSBuelRpbWU6IHN0cmluZztcblxuICBASG9zdExpc3RlbmVyKCdrZXl1cCcpXG4gIGtleXVwKCk6IHZvaWQge1xuICAgIHRoaXMuY2hhbmdlZCgpO1xuICB9XG5cbiAgQEhvc3RMaXN0ZW5lcignYmx1cicpXG4gIGJsdXIoKTogdm9pZCB7XG4gICAgdGhpcy50b3VjaGVkKCk7XG4gIH1cblxuICBjaGFuZ2VkKCk6IHZvaWQge1xuICAgIGlmICh0aGlzLl9vbkNoYW5nZSkge1xuICAgICAgY29uc3QgdmFsdWUgPSB0aGlzLmRhdGVIZWxwZXIucGFyc2VUaW1lKHRoaXMuZWxlbWVudFJlZi5uYXRpdmVFbGVtZW50LnZhbHVlKTtcbiAgICAgIHRoaXMuX29uQ2hhbmdlKHZhbHVlISk7XG4gICAgfVxuICB9XG5cbiAgdG91Y2hlZCgpOiB2b2lkIHtcbiAgICBpZiAodGhpcy5fb25Ub3VjaCkge1xuICAgICAgdGhpcy5fb25Ub3VjaCgpO1xuICAgIH1cbiAgfVxuXG4gIHNldFJhbmdlKCk6IHZvaWQge1xuICAgIHRoaXMuZWxlbWVudFJlZi5uYXRpdmVFbGVtZW50LmZvY3VzKCk7XG4gICAgdGhpcy5lbGVtZW50UmVmLm5hdGl2ZUVsZW1lbnQuc2V0U2VsZWN0aW9uUmFuZ2UoMCwgdGhpcy5lbGVtZW50UmVmLm5hdGl2ZUVsZW1lbnQudmFsdWUubGVuZ3RoKTtcbiAgfVxuXG4gIGNvbnN0cnVjdG9yKHByaXZhdGUgZGF0ZUhlbHBlcjogRGF0ZUhlbHBlclNlcnZpY2UsIHByaXZhdGUgZWxlbWVudFJlZjogRWxlbWVudFJlZikge31cblxuICB3cml0ZVZhbHVlKHZhbHVlOiBEYXRlKTogdm9pZCB7XG4gICAgdGhpcy5lbGVtZW50UmVmLm5hdGl2ZUVsZW1lbnQudmFsdWUgPSB0aGlzLmRhdGVIZWxwZXIuZm9ybWF0KHZhbHVlLCB0aGlzLm56VGltZSk7XG4gIH1cblxuICByZWdpc3Rlck9uQ2hhbmdlKGZuOiAodmFsdWU6IERhdGUpID0+IHZvaWQpOiB2b2lkIHtcbiAgICB0aGlzLl9vbkNoYW5nZSA9IGZuO1xuICB9XG5cbiAgcmVnaXN0ZXJPblRvdWNoZWQoZm46ICgpID0+IHZvaWQpOiB2b2lkIHtcbiAgICB0aGlzLl9vblRvdWNoID0gZm47XG4gIH1cbn1cbiJdfQ==