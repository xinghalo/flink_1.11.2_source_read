/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import * as tslib_1 from "tslib";
import { ChangeDetectionStrategy, Component, EventEmitter, Input, Output, TemplateRef, ViewEncapsulation } from '@angular/core';
import { InputBoolean } from '../core/util';
var NzRateItemComponent = /** @class */ (function () {
    function NzRateItemComponent() {
        this.allowHalf = false;
        this.itemHover = new EventEmitter();
        this.itemClick = new EventEmitter();
    }
    /**
     * @param {?} isHalf
     * @return {?}
     */
    NzRateItemComponent.prototype.hoverRate = /**
     * @param {?} isHalf
     * @return {?}
     */
    function (isHalf) {
        this.itemHover.next(isHalf && this.allowHalf);
    };
    /**
     * @param {?} isHalf
     * @return {?}
     */
    NzRateItemComponent.prototype.clickRate = /**
     * @param {?} isHalf
     * @return {?}
     */
    function (isHalf) {
        this.itemClick.next(isHalf && this.allowHalf);
    };
    NzRateItemComponent.decorators = [
        { type: Component, args: [{
                    changeDetection: ChangeDetectionStrategy.OnPush,
                    encapsulation: ViewEncapsulation.None,
                    selector: '[nz-rate-item]',
                    template: "<div class=\"ant-rate-star-second\"\n  (mouseover)=\"hoverRate(false); $event.stopPropagation();\"\n  (click)=\"clickRate(false); $event.stopPropagation();\">\n  <ng-template [ngTemplateOutlet]=\"character || defaultCharacter\"></ng-template>\n</div>\n<div class=\"ant-rate-star-first\"\n  (mouseover)=\"hoverRate(true); $event.stopPropagation();\"\n  (click)=\"clickRate(true); $event.stopPropagation();\">\n  <ng-template [ngTemplateOutlet]=\"character || defaultCharacter\"></ng-template>\n</div>\n\n<ng-template #defaultCharacter>\n  <i nz-icon\n    type=\"star\"\n    theme=\"fill\"></i>\n</ng-template>\n"
                }] }
    ];
    NzRateItemComponent.propDecorators = {
        character: [{ type: Input }],
        allowHalf: [{ type: Input }],
        itemHover: [{ type: Output }],
        itemClick: [{ type: Output }]
    };
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Boolean)
    ], NzRateItemComponent.prototype, "allowHalf", void 0);
    return NzRateItemComponent;
}());
export { NzRateItemComponent };
if (false) {
    /** @type {?} */
    NzRateItemComponent.prototype.character;
    /** @type {?} */
    NzRateItemComponent.prototype.allowHalf;
    /** @type {?} */
    NzRateItemComponent.prototype.itemHover;
    /** @type {?} */
    NzRateItemComponent.prototype.itemClick;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotcmF0ZS1pdGVtLmNvbXBvbmVudC5qcyIsInNvdXJjZVJvb3QiOiJuZzovL25nLXpvcnJvLWFudGQvIiwic291cmNlcyI6WyJyYXRlL256LXJhdGUtaXRlbS5jb21wb25lbnQudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7QUFBQSxPQUFPLEVBQ0wsdUJBQXVCLEVBQ3ZCLFNBQVMsRUFDVCxZQUFZLEVBQ1osS0FBSyxFQUNMLE1BQU0sRUFDTixXQUFXLEVBQ1gsaUJBQWlCLEVBQ2xCLE1BQU0sZUFBZSxDQUFDO0FBQ3ZCLE9BQU8sRUFBRSxZQUFZLEVBQUUsTUFBTSxjQUFjLENBQUM7QUFFNUM7SUFBQTtRQVEyQixjQUFTLEdBQVksS0FBSyxDQUFDO1FBQ2pDLGNBQVMsR0FBRyxJQUFJLFlBQVksRUFBVyxDQUFDO1FBQ3hDLGNBQVMsR0FBRyxJQUFJLFlBQVksRUFBVyxDQUFDO0lBUzdELENBQUM7Ozs7O0lBUEMsdUNBQVM7Ozs7SUFBVCxVQUFVLE1BQWU7UUFDdkIsSUFBSSxDQUFDLFNBQVMsQ0FBQyxJQUFJLENBQUMsTUFBTSxJQUFJLElBQUksQ0FBQyxTQUFTLENBQUMsQ0FBQztJQUNoRCxDQUFDOzs7OztJQUVELHVDQUFTOzs7O0lBQVQsVUFBVSxNQUFlO1FBQ3ZCLElBQUksQ0FBQyxTQUFTLENBQUMsSUFBSSxDQUFDLE1BQU0sSUFBSSxJQUFJLENBQUMsU0FBUyxDQUFDLENBQUM7SUFDaEQsQ0FBQzs7Z0JBbEJGLFNBQVMsU0FBQztvQkFDVCxlQUFlLEVBQUUsdUJBQXVCLENBQUMsTUFBTTtvQkFDL0MsYUFBYSxFQUFFLGlCQUFpQixDQUFDLElBQUk7b0JBQ3JDLFFBQVEsRUFBRSxnQkFBZ0I7b0JBQzFCLDhtQkFBNEM7aUJBQzdDOzs7NEJBRUUsS0FBSzs0QkFDTCxLQUFLOzRCQUNMLE1BQU07NEJBQ04sTUFBTTs7SUFGa0I7UUFBZixZQUFZLEVBQUU7OzBEQUE0QjtJQVd0RCwwQkFBQztDQUFBLEFBbkJELElBbUJDO1NBYlksbUJBQW1COzs7SUFDOUIsd0NBQXNDOztJQUN0Qyx3Q0FBb0Q7O0lBQ3BELHdDQUEyRDs7SUFDM0Qsd0NBQTJEIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHtcbiAgQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3ksXG4gIENvbXBvbmVudCxcbiAgRXZlbnRFbWl0dGVyLFxuICBJbnB1dCxcbiAgT3V0cHV0LFxuICBUZW1wbGF0ZVJlZixcbiAgVmlld0VuY2Fwc3VsYXRpb25cbn0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5pbXBvcnQgeyBJbnB1dEJvb2xlYW4gfSBmcm9tICcuLi9jb3JlL3V0aWwnO1xuXG5AQ29tcG9uZW50KHtcbiAgY2hhbmdlRGV0ZWN0aW9uOiBDaGFuZ2VEZXRlY3Rpb25TdHJhdGVneS5PblB1c2gsXG4gIGVuY2Fwc3VsYXRpb246IFZpZXdFbmNhcHN1bGF0aW9uLk5vbmUsXG4gIHNlbGVjdG9yOiAnW256LXJhdGUtaXRlbV0nLFxuICB0ZW1wbGF0ZVVybDogJy4vbnotcmF0ZS1pdGVtLmNvbXBvbmVudC5odG1sJ1xufSlcbmV4cG9ydCBjbGFzcyBOelJhdGVJdGVtQ29tcG9uZW50IHtcbiAgQElucHV0KCkgY2hhcmFjdGVyOiBUZW1wbGF0ZVJlZjx2b2lkPjtcbiAgQElucHV0KCkgQElucHV0Qm9vbGVhbigpIGFsbG93SGFsZjogYm9vbGVhbiA9IGZhbHNlO1xuICBAT3V0cHV0KCkgcmVhZG9ubHkgaXRlbUhvdmVyID0gbmV3IEV2ZW50RW1pdHRlcjxib29sZWFuPigpO1xuICBAT3V0cHV0KCkgcmVhZG9ubHkgaXRlbUNsaWNrID0gbmV3IEV2ZW50RW1pdHRlcjxib29sZWFuPigpO1xuXG4gIGhvdmVyUmF0ZShpc0hhbGY6IGJvb2xlYW4pOiB2b2lkIHtcbiAgICB0aGlzLml0ZW1Ib3Zlci5uZXh0KGlzSGFsZiAmJiB0aGlzLmFsbG93SGFsZik7XG4gIH1cblxuICBjbGlja1JhdGUoaXNIYWxmOiBib29sZWFuKTogdm9pZCB7XG4gICAgdGhpcy5pdGVtQ2xpY2submV4dChpc0hhbGYgJiYgdGhpcy5hbGxvd0hhbGYpO1xuICB9XG59XG4iXX0=