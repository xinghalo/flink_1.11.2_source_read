/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { DOCUMENT } from '@angular/common';
import { Inject, Injectable } from '@angular/core';
import { isNotNil } from '../util/check';
import * as i0 from "@angular/core";
import * as i1 from "@angular/common";
var NzMeasureScrollbarService = /** @class */ (function () {
    // tslint:disable-next-line:no-any
    function NzMeasureScrollbarService(document) {
        this.document = document;
        this.scrollbarMeasure = {
            position: 'absolute',
            top: '-9999px',
            width: '50px',
            height: '50px',
            overflow: 'scroll'
        };
        this.initScrollBarWidth();
    }
    Object.defineProperty(NzMeasureScrollbarService.prototype, "scrollBarWidth", {
        get: /**
         * @return {?}
         */
        function () {
            if (isNotNil(this._scrollbarWidth)) {
                return this._scrollbarWidth;
            }
            this.initScrollBarWidth();
            return this._scrollbarWidth;
        },
        enumerable: true,
        configurable: true
    });
    /**
     * @return {?}
     */
    NzMeasureScrollbarService.prototype.initScrollBarWidth = /**
     * @return {?}
     */
    function () {
        /** @type {?} */
        var scrollDiv = this.document.createElement('div');
        for (var scrollProp in this.scrollbarMeasure) {
            if (this.scrollbarMeasure.hasOwnProperty(scrollProp)) {
                scrollDiv.style[scrollProp] = this.scrollbarMeasure[scrollProp];
            }
        }
        this.document.body.appendChild(scrollDiv);
        /** @type {?} */
        var width = scrollDiv.offsetWidth - scrollDiv.clientWidth;
        this.document.body.removeChild(scrollDiv);
        this._scrollbarWidth = width;
    };
    NzMeasureScrollbarService.decorators = [
        { type: Injectable, args: [{
                    providedIn: 'root'
                },] }
    ];
    /** @nocollapse */
    NzMeasureScrollbarService.ctorParameters = function () { return [
        { type: undefined, decorators: [{ type: Inject, args: [DOCUMENT,] }] }
    ]; };
    /** @nocollapse */ NzMeasureScrollbarService.ngInjectableDef = i0.defineInjectable({ factory: function NzMeasureScrollbarService_Factory() { return new NzMeasureScrollbarService(i0.inject(i1.DOCUMENT)); }, token: NzMeasureScrollbarService, providedIn: "root" });
    return NzMeasureScrollbarService;
}());
export { NzMeasureScrollbarService };
if (false) {
    /**
     * @type {?}
     * @private
     */
    NzMeasureScrollbarService.prototype._scrollbarWidth;
    /**
     * @type {?}
     * @private
     */
    NzMeasureScrollbarService.prototype.scrollbarMeasure;
    /**
     * @type {?}
     * @private
     */
    NzMeasureScrollbarService.prototype.document;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotbWVhc3VyZS1zY3JvbGxiYXIuc2VydmljZS5qcyIsInNvdXJjZVJvb3QiOiJuZzovL25nLXpvcnJvLWFudGQvIiwic291cmNlcyI6WyJjb3JlL3NlcnZpY2VzL256LW1lYXN1cmUtc2Nyb2xsYmFyLnNlcnZpY2UudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7OztBQUFBLE9BQU8sRUFBRSxRQUFRLEVBQUUsTUFBTSxpQkFBaUIsQ0FBQztBQUMzQyxPQUFPLEVBQUUsTUFBTSxFQUFFLFVBQVUsRUFBRSxNQUFNLGVBQWUsQ0FBQztBQUVuRCxPQUFPLEVBQUUsUUFBUSxFQUFFLE1BQU0sZUFBZSxDQUFDOzs7QUFFekM7SUFrQ0Usa0NBQWtDO0lBQ2xDLG1DQUFzQyxRQUFhO1FBQWIsYUFBUSxHQUFSLFFBQVEsQ0FBSztRQTlCM0MscUJBQWdCLEdBQW9CO1lBQzFDLFFBQVEsRUFBRSxVQUFVO1lBQ3BCLEdBQUcsRUFBTyxTQUFTO1lBQ25CLEtBQUssRUFBSyxNQUFNO1lBQ2hCLE1BQU0sRUFBSSxNQUFNO1lBQ2hCLFFBQVEsRUFBRSxRQUFRO1NBQ25CLENBQUM7UUF5QkEsSUFBSSxDQUFDLGtCQUFrQixFQUFFLENBQUM7SUFDNUIsQ0FBQztJQXhCRCxzQkFBSSxxREFBYzs7OztRQUFsQjtZQUNFLElBQUksUUFBUSxDQUFDLElBQUksQ0FBQyxlQUFlLENBQUMsRUFBRTtnQkFDbEMsT0FBTyxJQUFJLENBQUMsZUFBZSxDQUFDO2FBQzdCO1lBQ0QsSUFBSSxDQUFDLGtCQUFrQixFQUFFLENBQUM7WUFDMUIsT0FBTyxJQUFJLENBQUMsZUFBZSxDQUFDO1FBQzlCLENBQUM7OztPQUFBOzs7O0lBRUQsc0RBQWtCOzs7SUFBbEI7O1lBQ1EsU0FBUyxHQUFHLElBQUksQ0FBQyxRQUFRLENBQUMsYUFBYSxDQUFDLEtBQUssQ0FBQztRQUNwRCxLQUFLLElBQU0sVUFBVSxJQUFJLElBQUksQ0FBQyxnQkFBZ0IsRUFBRTtZQUM5QyxJQUFJLElBQUksQ0FBQyxnQkFBZ0IsQ0FBQyxjQUFjLENBQUMsVUFBVSxDQUFDLEVBQUU7Z0JBQ3BELFNBQVMsQ0FBQyxLQUFLLENBQUUsVUFBVSxDQUFFLEdBQUcsSUFBSSxDQUFDLGdCQUFnQixDQUFFLFVBQVUsQ0FBRSxDQUFDO2FBQ3JFO1NBQ0Y7UUFDRCxJQUFJLENBQUMsUUFBUSxDQUFDLElBQUksQ0FBQyxXQUFXLENBQUMsU0FBUyxDQUFDLENBQUM7O1lBQ3BDLEtBQUssR0FBRyxTQUFTLENBQUMsV0FBVyxHQUFHLFNBQVMsQ0FBQyxXQUFXO1FBQzNELElBQUksQ0FBQyxRQUFRLENBQUMsSUFBSSxDQUFDLFdBQVcsQ0FBQyxTQUFTLENBQUMsQ0FBQztRQUMxQyxJQUFJLENBQUMsZUFBZSxHQUFHLEtBQUssQ0FBQztJQUMvQixDQUFDOztnQkFoQ0YsVUFBVSxTQUFDO29CQUNWLFVBQVUsRUFBRSxNQUFNO2lCQUNuQjs7OztnREFpQ2MsTUFBTSxTQUFDLFFBQVE7OztvQ0F4QzlCO0NBMkNDLEFBdENELElBc0NDO1NBbkNZLHlCQUF5Qjs7Ozs7O0lBQ3BDLG9EQUFnQzs7Ozs7SUFDaEMscURBTUU7Ozs7O0lBd0JVLDZDQUF1QyIsInNvdXJjZXNDb250ZW50IjpbImltcG9ydCB7IERPQ1VNRU5UIH0gZnJvbSAnQGFuZ3VsYXIvY29tbW9uJztcbmltcG9ydCB7IEluamVjdCwgSW5qZWN0YWJsZSB9IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuaW1wb3J0IHsgSW5kZXhhYmxlT2JqZWN0IH0gZnJvbSAnLi4vdHlwZXMvaW5kZXhhYmxlJztcbmltcG9ydCB7IGlzTm90TmlsIH0gZnJvbSAnLi4vdXRpbC9jaGVjayc7XG5cbkBJbmplY3RhYmxlKHtcbiAgcHJvdmlkZWRJbjogJ3Jvb3QnXG59KVxuZXhwb3J0IGNsYXNzIE56TWVhc3VyZVNjcm9sbGJhclNlcnZpY2Uge1xuICBwcml2YXRlIF9zY3JvbGxiYXJXaWR0aDogbnVtYmVyO1xuICBwcml2YXRlIHNjcm9sbGJhck1lYXN1cmU6IEluZGV4YWJsZU9iamVjdCA9IHtcbiAgICBwb3NpdGlvbjogJ2Fic29sdXRlJyxcbiAgICB0b3AgICAgIDogJy05OTk5cHgnLFxuICAgIHdpZHRoICAgOiAnNTBweCcsXG4gICAgaGVpZ2h0ICA6ICc1MHB4JyxcbiAgICBvdmVyZmxvdzogJ3Njcm9sbCdcbiAgfTtcblxuICBnZXQgc2Nyb2xsQmFyV2lkdGgoKTogbnVtYmVyIHtcbiAgICBpZiAoaXNOb3ROaWwodGhpcy5fc2Nyb2xsYmFyV2lkdGgpKSB7XG4gICAgICByZXR1cm4gdGhpcy5fc2Nyb2xsYmFyV2lkdGg7XG4gICAgfVxuICAgIHRoaXMuaW5pdFNjcm9sbEJhcldpZHRoKCk7XG4gICAgcmV0dXJuIHRoaXMuX3Njcm9sbGJhcldpZHRoO1xuICB9XG5cbiAgaW5pdFNjcm9sbEJhcldpZHRoKCk6IHZvaWQge1xuICAgIGNvbnN0IHNjcm9sbERpdiA9IHRoaXMuZG9jdW1lbnQuY3JlYXRlRWxlbWVudCgnZGl2Jyk7XG4gICAgZm9yIChjb25zdCBzY3JvbGxQcm9wIGluIHRoaXMuc2Nyb2xsYmFyTWVhc3VyZSkge1xuICAgICAgaWYgKHRoaXMuc2Nyb2xsYmFyTWVhc3VyZS5oYXNPd25Qcm9wZXJ0eShzY3JvbGxQcm9wKSkge1xuICAgICAgICBzY3JvbGxEaXYuc3R5bGVbIHNjcm9sbFByb3AgXSA9IHRoaXMuc2Nyb2xsYmFyTWVhc3VyZVsgc2Nyb2xsUHJvcCBdO1xuICAgICAgfVxuICAgIH1cbiAgICB0aGlzLmRvY3VtZW50LmJvZHkuYXBwZW5kQ2hpbGQoc2Nyb2xsRGl2KTtcbiAgICBjb25zdCB3aWR0aCA9IHNjcm9sbERpdi5vZmZzZXRXaWR0aCAtIHNjcm9sbERpdi5jbGllbnRXaWR0aDtcbiAgICB0aGlzLmRvY3VtZW50LmJvZHkucmVtb3ZlQ2hpbGQoc2Nyb2xsRGl2KTtcbiAgICB0aGlzLl9zY3JvbGxiYXJXaWR0aCA9IHdpZHRoO1xuICB9XG5cbiAgLy8gdHNsaW50OmRpc2FibGUtbmV4dC1saW5lOm5vLWFueVxuICBjb25zdHJ1Y3RvcihASW5qZWN0KERPQ1VNRU5UKSBwcml2YXRlIGRvY3VtZW50OiBhbnkpIHtcbiAgICB0aGlzLmluaXRTY3JvbGxCYXJXaWR0aCgpO1xuICB9XG59XG4iXX0=