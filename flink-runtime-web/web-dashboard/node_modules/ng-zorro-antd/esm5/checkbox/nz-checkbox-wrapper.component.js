/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { ChangeDetectionStrategy, Component, ElementRef, EventEmitter, Output, Renderer2, ViewEncapsulation } from '@angular/core';
var NzCheckboxWrapperComponent = /** @class */ (function () {
    function NzCheckboxWrapperComponent(renderer, elementRef) {
        this.nzOnChange = new EventEmitter();
        this.checkboxList = [];
        renderer.addClass(elementRef.nativeElement, 'ant-checkbox-group');
    }
    /**
     * @param {?} value
     * @return {?}
     */
    NzCheckboxWrapperComponent.prototype.addCheckbox = /**
     * @param {?} value
     * @return {?}
     */
    function (value) {
        this.checkboxList.push(value);
    };
    /**
     * @param {?} value
     * @return {?}
     */
    NzCheckboxWrapperComponent.prototype.removeCheckbox = /**
     * @param {?} value
     * @return {?}
     */
    function (value) {
        this.checkboxList.splice(this.checkboxList.indexOf(value), 1);
    };
    /**
     * @return {?}
     */
    NzCheckboxWrapperComponent.prototype.outputValue = /**
     * @return {?}
     */
    function () {
        /** @type {?} */
        var checkedList = this.checkboxList.filter((/**
         * @param {?} item
         * @return {?}
         */
        function (item) { return item.nzChecked; }));
        return checkedList.map((/**
         * @param {?} item
         * @return {?}
         */
        function (item) { return item.nzValue; }));
    };
    /**
     * @return {?}
     */
    NzCheckboxWrapperComponent.prototype.onChange = /**
     * @return {?}
     */
    function () {
        this.nzOnChange.emit(this.outputValue());
    };
    NzCheckboxWrapperComponent.decorators = [
        { type: Component, args: [{
                    selector: 'nz-checkbox-wrapper',
                    preserveWhitespaces: false,
                    changeDetection: ChangeDetectionStrategy.OnPush,
                    encapsulation: ViewEncapsulation.None,
                    template: "<ng-content></ng-content>"
                }] }
    ];
    /** @nocollapse */
    NzCheckboxWrapperComponent.ctorParameters = function () { return [
        { type: Renderer2 },
        { type: ElementRef }
    ]; };
    NzCheckboxWrapperComponent.propDecorators = {
        nzOnChange: [{ type: Output }]
    };
    return NzCheckboxWrapperComponent;
}());
export { NzCheckboxWrapperComponent };
if (false) {
    /** @type {?} */
    NzCheckboxWrapperComponent.prototype.nzOnChange;
    /**
     * @type {?}
     * @private
     */
    NzCheckboxWrapperComponent.prototype.checkboxList;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotY2hlY2tib3gtd3JhcHBlci5jb21wb25lbnQuanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsiY2hlY2tib3gvbnotY2hlY2tib3gtd3JhcHBlci5jb21wb25lbnQudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7OztBQUFBLE9BQU8sRUFDTCx1QkFBdUIsRUFDdkIsU0FBUyxFQUNULFVBQVUsRUFDVixZQUFZLEVBQ1osTUFBTSxFQUNOLFNBQVMsRUFDVCxpQkFBaUIsRUFDbEIsTUFBTSxlQUFlLENBQUM7QUFJdkI7SUE0QkUsb0NBQVksUUFBbUIsRUFBRSxVQUFzQjtRQXBCcEMsZUFBVSxHQUFHLElBQUksWUFBWSxFQUFZLENBQUM7UUFDckQsaUJBQVksR0FBMEIsRUFBRSxDQUFDO1FBb0IvQyxRQUFRLENBQUMsUUFBUSxDQUFDLFVBQVUsQ0FBQyxhQUFhLEVBQUUsb0JBQW9CLENBQUMsQ0FBQztJQUNwRSxDQUFDOzs7OztJQW5CRCxnREFBVzs7OztJQUFYLFVBQVksS0FBMEI7UUFDcEMsSUFBSSxDQUFDLFlBQVksQ0FBQyxJQUFJLENBQUMsS0FBSyxDQUFDLENBQUM7SUFDaEMsQ0FBQzs7Ozs7SUFFRCxtREFBYzs7OztJQUFkLFVBQWUsS0FBMEI7UUFDdkMsSUFBSSxDQUFDLFlBQVksQ0FBQyxNQUFNLENBQUMsSUFBSSxDQUFDLFlBQVksQ0FBQyxPQUFPLENBQUMsS0FBSyxDQUFDLEVBQUUsQ0FBQyxDQUFDLENBQUM7SUFDaEUsQ0FBQzs7OztJQUVELGdEQUFXOzs7SUFBWDs7WUFDUSxXQUFXLEdBQUcsSUFBSSxDQUFDLFlBQVksQ0FBQyxNQUFNOzs7O1FBQUMsVUFBQSxJQUFJLElBQUksT0FBQSxJQUFJLENBQUMsU0FBUyxFQUFkLENBQWMsRUFBQztRQUNwRSxPQUFPLFdBQVcsQ0FBQyxHQUFHOzs7O1FBQUMsVUFBQSxJQUFJLElBQUksT0FBQSxJQUFJLENBQUMsT0FBTyxFQUFaLENBQVksRUFBQyxDQUFDO0lBQy9DLENBQUM7Ozs7SUFFRCw2Q0FBUTs7O0lBQVI7UUFDRSxJQUFJLENBQUMsVUFBVSxDQUFDLElBQUksQ0FBQyxJQUFJLENBQUMsV0FBVyxFQUFFLENBQUMsQ0FBQztJQUMzQyxDQUFDOztnQkExQkYsU0FBUyxTQUFDO29CQUNULFFBQVEsRUFBRSxxQkFBcUI7b0JBQy9CLG1CQUFtQixFQUFFLEtBQUs7b0JBQzFCLGVBQWUsRUFBRSx1QkFBdUIsQ0FBQyxNQUFNO29CQUMvQyxhQUFhLEVBQUUsaUJBQWlCLENBQUMsSUFBSTtvQkFDckMscUNBQW1EO2lCQUNwRDs7OztnQkFaQyxTQUFTO2dCQUhULFVBQVU7Ozs2QkFpQlQsTUFBTTs7SUF1QlQsaUNBQUM7Q0FBQSxBQS9CRCxJQStCQztTQXhCWSwwQkFBMEI7OztJQUNyQyxnREFBNkQ7Ozs7O0lBQzdELGtEQUFpRCIsInNvdXJjZXNDb250ZW50IjpbImltcG9ydCB7XG4gIENoYW5nZURldGVjdGlvblN0cmF0ZWd5LFxuICBDb21wb25lbnQsXG4gIEVsZW1lbnRSZWYsXG4gIEV2ZW50RW1pdHRlcixcbiAgT3V0cHV0LFxuICBSZW5kZXJlcjIsXG4gIFZpZXdFbmNhcHN1bGF0aW9uXG59IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuXG5pbXBvcnQgeyBOekNoZWNrYm94Q29tcG9uZW50IH0gZnJvbSAnLi9uei1jaGVja2JveC5jb21wb25lbnQnO1xuXG5AQ29tcG9uZW50KHtcbiAgc2VsZWN0b3I6ICduei1jaGVja2JveC13cmFwcGVyJyxcbiAgcHJlc2VydmVXaGl0ZXNwYWNlczogZmFsc2UsXG4gIGNoYW5nZURldGVjdGlvbjogQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3kuT25QdXNoLFxuICBlbmNhcHN1bGF0aW9uOiBWaWV3RW5jYXBzdWxhdGlvbi5Ob25lLFxuICB0ZW1wbGF0ZVVybDogJy4vbnotY2hlY2tib3gtd3JhcHBlci5jb21wb25lbnQuaHRtbCdcbn0pXG5leHBvcnQgY2xhc3MgTnpDaGVja2JveFdyYXBwZXJDb21wb25lbnQge1xuICBAT3V0cHV0KCkgcmVhZG9ubHkgbnpPbkNoYW5nZSA9IG5ldyBFdmVudEVtaXR0ZXI8c3RyaW5nW10+KCk7XG4gIHByaXZhdGUgY2hlY2tib3hMaXN0OiBOekNoZWNrYm94Q29tcG9uZW50W10gPSBbXTtcblxuICBhZGRDaGVja2JveCh2YWx1ZTogTnpDaGVja2JveENvbXBvbmVudCk6IHZvaWQge1xuICAgIHRoaXMuY2hlY2tib3hMaXN0LnB1c2godmFsdWUpO1xuICB9XG5cbiAgcmVtb3ZlQ2hlY2tib3godmFsdWU6IE56Q2hlY2tib3hDb21wb25lbnQpOiB2b2lkIHtcbiAgICB0aGlzLmNoZWNrYm94TGlzdC5zcGxpY2UodGhpcy5jaGVja2JveExpc3QuaW5kZXhPZih2YWx1ZSksIDEpO1xuICB9XG5cbiAgb3V0cHV0VmFsdWUoKTogc3RyaW5nW10ge1xuICAgIGNvbnN0IGNoZWNrZWRMaXN0ID0gdGhpcy5jaGVja2JveExpc3QuZmlsdGVyKGl0ZW0gPT4gaXRlbS5uekNoZWNrZWQpO1xuICAgIHJldHVybiBjaGVja2VkTGlzdC5tYXAoaXRlbSA9PiBpdGVtLm56VmFsdWUpO1xuICB9XG5cbiAgb25DaGFuZ2UoKTogdm9pZCB7XG4gICAgdGhpcy5uek9uQ2hhbmdlLmVtaXQodGhpcy5vdXRwdXRWYWx1ZSgpKTtcbiAgfVxuXG4gIGNvbnN0cnVjdG9yKHJlbmRlcmVyOiBSZW5kZXJlcjIsIGVsZW1lbnRSZWY6IEVsZW1lbnRSZWYpIHtcbiAgICByZW5kZXJlci5hZGRDbGFzcyhlbGVtZW50UmVmLm5hdGl2ZUVsZW1lbnQsICdhbnQtY2hlY2tib3gtZ3JvdXAnKTtcbiAgfVxufVxuIl19