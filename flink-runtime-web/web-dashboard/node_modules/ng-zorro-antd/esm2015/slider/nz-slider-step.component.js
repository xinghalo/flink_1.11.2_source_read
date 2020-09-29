/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import * as tslib_1 from "tslib";
import { ChangeDetectionStrategy, Component, Input, ViewEncapsulation } from '@angular/core';
import { InputBoolean } from '../core/util/convert';
export class NzSliderStepComponent {
    constructor() {
        this.nzLowerBound = null;
        this.nzUpperBound = null;
        this.nzVertical = false;
        this.nzIncluded = false;
    }
    /**
     * @param {?} changes
     * @return {?}
     */
    ngOnChanges(changes) {
        if (changes.nzMarksArray) {
            this.buildSteps();
        }
        if (changes.nzMarksArray || changes.nzLowerBound || changes.nzUpperBound) {
            this.togglePointActive();
        }
    }
    /**
     * @param {?} _index
     * @param {?} step
     * @return {?}
     */
    trackById(_index, step) {
        return step.value;
    }
    /**
     * @private
     * @return {?}
     */
    buildSteps() {
        /** @type {?} */
        const orient = this.nzVertical ? 'bottom' : 'left';
        this.steps = this.nzMarksArray.map((/**
         * @param {?} mark
         * @return {?}
         */
        mark => {
            const { value, offset, config } = mark;
            return {
                value,
                offset,
                config,
                active: false,
                style: {
                    [orient]: `${offset}%`
                }
            };
        }));
    }
    /**
     * @private
     * @return {?}
     */
    togglePointActive() {
        if (this.steps && this.nzLowerBound !== null && this.nzUpperBound !== null) {
            this.steps.forEach((/**
             * @param {?} step
             * @return {?}
             */
            step => {
                /** @type {?} */
                const value = step.value;
                /** @type {?} */
                const isActive = (!this.nzIncluded && value === this.nzUpperBound) ||
                    (this.nzIncluded && value <= (/** @type {?} */ (this.nzUpperBound)) && value >= (/** @type {?} */ (this.nzLowerBound)));
                step.active = isActive;
            }));
        }
    }
}
NzSliderStepComponent.decorators = [
    { type: Component, args: [{
                changeDetection: ChangeDetectionStrategy.OnPush,
                encapsulation: ViewEncapsulation.None,
                selector: 'nz-slider-step',
                preserveWhitespaces: false,
                template: "<div class=\"ant-slider-step\">\n  <span\n    class=\"ant-slider-dot\"\n    *ngFor=\"let mark of steps; trackBy: trackById\"\n    [class.ant-slider-dot-active]=\"mark.active\"\n    [ngStyle]=\"mark.style\">\n  </span>\n</div>"
            }] }
];
NzSliderStepComponent.propDecorators = {
    nzLowerBound: [{ type: Input }],
    nzUpperBound: [{ type: Input }],
    nzMarksArray: [{ type: Input }],
    nzVertical: [{ type: Input }],
    nzIncluded: [{ type: Input }]
};
tslib_1.__decorate([
    InputBoolean(),
    tslib_1.__metadata("design:type", Object)
], NzSliderStepComponent.prototype, "nzVertical", void 0);
tslib_1.__decorate([
    InputBoolean(),
    tslib_1.__metadata("design:type", Object)
], NzSliderStepComponent.prototype, "nzIncluded", void 0);
if (false) {
    /** @type {?} */
    NzSliderStepComponent.prototype.nzLowerBound;
    /** @type {?} */
    NzSliderStepComponent.prototype.nzUpperBound;
    /** @type {?} */
    NzSliderStepComponent.prototype.nzMarksArray;
    /** @type {?} */
    NzSliderStepComponent.prototype.nzVertical;
    /** @type {?} */
    NzSliderStepComponent.prototype.nzIncluded;
    /** @type {?} */
    NzSliderStepComponent.prototype.steps;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotc2xpZGVyLXN0ZXAuY29tcG9uZW50LmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbInNsaWRlci9uei1zbGlkZXItc3RlcC5jb21wb25lbnQudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7QUFBQSxPQUFPLEVBQUUsdUJBQXVCLEVBQUUsU0FBUyxFQUFFLEtBQUssRUFBNEIsaUJBQWlCLEVBQUUsTUFBTSxlQUFlLENBQUM7QUFFdkgsT0FBTyxFQUFFLFlBQVksRUFBRSxNQUFNLHNCQUFzQixDQUFDO0FBV3BELE1BQU0sT0FBTyxxQkFBcUI7SUFQbEM7UUFRVyxpQkFBWSxHQUFrQixJQUFJLENBQUM7UUFDbkMsaUJBQVksR0FBa0IsSUFBSSxDQUFDO1FBRW5CLGVBQVUsR0FBRyxLQUFLLENBQUM7UUFDbkIsZUFBVSxHQUFHLEtBQUssQ0FBQztJQThDOUMsQ0FBQzs7Ozs7SUExQ0MsV0FBVyxDQUFDLE9BQXNCO1FBQ2hDLElBQUksT0FBTyxDQUFDLFlBQVksRUFBRTtZQUN4QixJQUFJLENBQUMsVUFBVSxFQUFFLENBQUM7U0FDbkI7UUFDRCxJQUFJLE9BQU8sQ0FBQyxZQUFZLElBQUksT0FBTyxDQUFDLFlBQVksSUFBSSxPQUFPLENBQUMsWUFBWSxFQUFFO1lBQ3hFLElBQUksQ0FBQyxpQkFBaUIsRUFBRSxDQUFDO1NBQzFCO0lBQ0gsQ0FBQzs7Ozs7O0lBRUQsU0FBUyxDQUFDLE1BQWMsRUFBRSxJQUFtQjtRQUMzQyxPQUFPLElBQUksQ0FBQyxLQUFLLENBQUM7SUFDcEIsQ0FBQzs7Ozs7SUFFTyxVQUFVOztjQUNWLE1BQU0sR0FBRyxJQUFJLENBQUMsVUFBVSxDQUFDLENBQUMsQ0FBQyxRQUFRLENBQUMsQ0FBQyxDQUFDLE1BQU07UUFFbEQsSUFBSSxDQUFDLEtBQUssR0FBRyxJQUFJLENBQUMsWUFBWSxDQUFDLEdBQUc7Ozs7UUFBQyxJQUFJLENBQUMsRUFBRTtrQkFDbEMsRUFBRSxLQUFLLEVBQUUsTUFBTSxFQUFFLE1BQU0sRUFBRSxHQUFHLElBQUk7WUFFdEMsT0FBTztnQkFDTCxLQUFLO2dCQUNMLE1BQU07Z0JBQ04sTUFBTTtnQkFDTixNQUFNLEVBQUUsS0FBSztnQkFDYixLQUFLLEVBQUU7b0JBQ0wsQ0FBQyxNQUFNLENBQUMsRUFBRSxHQUFHLE1BQU0sR0FBRztpQkFDdkI7YUFDRixDQUFDO1FBQ0osQ0FBQyxFQUFDLENBQUM7SUFDTCxDQUFDOzs7OztJQUVPLGlCQUFpQjtRQUN2QixJQUFJLElBQUksQ0FBQyxLQUFLLElBQUksSUFBSSxDQUFDLFlBQVksS0FBSyxJQUFJLElBQUksSUFBSSxDQUFDLFlBQVksS0FBSyxJQUFJLEVBQUU7WUFDMUUsSUFBSSxDQUFDLEtBQUssQ0FBQyxPQUFPOzs7O1lBQUMsSUFBSSxDQUFDLEVBQUU7O3NCQUNsQixLQUFLLEdBQUcsSUFBSSxDQUFDLEtBQUs7O3NCQUNsQixRQUFRLEdBQ1osQ0FBQyxDQUFDLElBQUksQ0FBQyxVQUFVLElBQUksS0FBSyxLQUFLLElBQUksQ0FBQyxZQUFZLENBQUM7b0JBQ2pELENBQUMsSUFBSSxDQUFDLFVBQVUsSUFBSSxLQUFLLElBQUksbUJBQUEsSUFBSSxDQUFDLFlBQVksRUFBQyxJQUFJLEtBQUssSUFBSSxtQkFBQSxJQUFJLENBQUMsWUFBWSxFQUFDLENBQUM7Z0JBQ2pGLElBQUksQ0FBQyxNQUFNLEdBQUcsUUFBUSxDQUFDO1lBQ3pCLENBQUMsRUFBQyxDQUFDO1NBQ0o7SUFDSCxDQUFDOzs7WUF6REYsU0FBUyxTQUFDO2dCQUNULGVBQWUsRUFBRSx1QkFBdUIsQ0FBQyxNQUFNO2dCQUMvQyxhQUFhLEVBQUUsaUJBQWlCLENBQUMsSUFBSTtnQkFDckMsUUFBUSxFQUFFLGdCQUFnQjtnQkFDMUIsbUJBQW1CLEVBQUUsS0FBSztnQkFDMUIsNk9BQThDO2FBQy9DOzs7MkJBRUUsS0FBSzsyQkFDTCxLQUFLOzJCQUNMLEtBQUs7eUJBQ0wsS0FBSzt5QkFDTCxLQUFLOztBQURtQjtJQUFmLFlBQVksRUFBRTs7eURBQW9CO0FBQ25CO0lBQWYsWUFBWSxFQUFFOzt5REFBb0I7OztJQUo1Qyw2Q0FBNEM7O0lBQzVDLDZDQUE0Qzs7SUFDNUMsNkNBQXNDOztJQUN0QywyQ0FBNEM7O0lBQzVDLDJDQUE0Qzs7SUFFNUMsc0NBQXVCIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHsgQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3ksIENvbXBvbmVudCwgSW5wdXQsIE9uQ2hhbmdlcywgU2ltcGxlQ2hhbmdlcywgVmlld0VuY2Fwc3VsYXRpb24gfSBmcm9tICdAYW5ndWxhci9jb3JlJztcblxuaW1wb3J0IHsgSW5wdXRCb29sZWFuIH0gZnJvbSAnLi4vY29yZS91dGlsL2NvbnZlcnQnO1xuXG5pbXBvcnQgeyBEaXNwbGF5ZWRTdGVwLCBFeHRlbmRlZE1hcmsgfSBmcm9tICcuL256LXNsaWRlci1kZWZpbml0aW9ucyc7XG5cbkBDb21wb25lbnQoe1xuICBjaGFuZ2VEZXRlY3Rpb246IENoYW5nZURldGVjdGlvblN0cmF0ZWd5Lk9uUHVzaCxcbiAgZW5jYXBzdWxhdGlvbjogVmlld0VuY2Fwc3VsYXRpb24uTm9uZSxcbiAgc2VsZWN0b3I6ICduei1zbGlkZXItc3RlcCcsXG4gIHByZXNlcnZlV2hpdGVzcGFjZXM6IGZhbHNlLFxuICB0ZW1wbGF0ZVVybDogJy4vbnotc2xpZGVyLXN0ZXAuY29tcG9uZW50Lmh0bWwnXG59KVxuZXhwb3J0IGNsYXNzIE56U2xpZGVyU3RlcENvbXBvbmVudCBpbXBsZW1lbnRzIE9uQ2hhbmdlcyB7XG4gIEBJbnB1dCgpIG56TG93ZXJCb3VuZDogbnVtYmVyIHwgbnVsbCA9IG51bGw7XG4gIEBJbnB1dCgpIG56VXBwZXJCb3VuZDogbnVtYmVyIHwgbnVsbCA9IG51bGw7XG4gIEBJbnB1dCgpIG56TWFya3NBcnJheTogRXh0ZW5kZWRNYXJrW107XG4gIEBJbnB1dCgpIEBJbnB1dEJvb2xlYW4oKSBuelZlcnRpY2FsID0gZmFsc2U7XG4gIEBJbnB1dCgpIEBJbnB1dEJvb2xlYW4oKSBuekluY2x1ZGVkID0gZmFsc2U7XG5cbiAgc3RlcHM6IERpc3BsYXllZFN0ZXBbXTtcblxuICBuZ09uQ2hhbmdlcyhjaGFuZ2VzOiBTaW1wbGVDaGFuZ2VzKTogdm9pZCB7XG4gICAgaWYgKGNoYW5nZXMubnpNYXJrc0FycmF5KSB7XG4gICAgICB0aGlzLmJ1aWxkU3RlcHMoKTtcbiAgICB9XG4gICAgaWYgKGNoYW5nZXMubnpNYXJrc0FycmF5IHx8IGNoYW5nZXMubnpMb3dlckJvdW5kIHx8IGNoYW5nZXMubnpVcHBlckJvdW5kKSB7XG4gICAgICB0aGlzLnRvZ2dsZVBvaW50QWN0aXZlKCk7XG4gICAgfVxuICB9XG5cbiAgdHJhY2tCeUlkKF9pbmRleDogbnVtYmVyLCBzdGVwOiBEaXNwbGF5ZWRTdGVwKTogbnVtYmVyIHtcbiAgICByZXR1cm4gc3RlcC52YWx1ZTtcbiAgfVxuXG4gIHByaXZhdGUgYnVpbGRTdGVwcygpOiB2b2lkIHtcbiAgICBjb25zdCBvcmllbnQgPSB0aGlzLm56VmVydGljYWwgPyAnYm90dG9tJyA6ICdsZWZ0JztcblxuICAgIHRoaXMuc3RlcHMgPSB0aGlzLm56TWFya3NBcnJheS5tYXAobWFyayA9PiB7XG4gICAgICBjb25zdCB7IHZhbHVlLCBvZmZzZXQsIGNvbmZpZyB9ID0gbWFyaztcblxuICAgICAgcmV0dXJuIHtcbiAgICAgICAgdmFsdWUsXG4gICAgICAgIG9mZnNldCxcbiAgICAgICAgY29uZmlnLFxuICAgICAgICBhY3RpdmU6IGZhbHNlLFxuICAgICAgICBzdHlsZToge1xuICAgICAgICAgIFtvcmllbnRdOiBgJHtvZmZzZXR9JWBcbiAgICAgICAgfVxuICAgICAgfTtcbiAgICB9KTtcbiAgfVxuXG4gIHByaXZhdGUgdG9nZ2xlUG9pbnRBY3RpdmUoKTogdm9pZCB7XG4gICAgaWYgKHRoaXMuc3RlcHMgJiYgdGhpcy5uekxvd2VyQm91bmQgIT09IG51bGwgJiYgdGhpcy5uelVwcGVyQm91bmQgIT09IG51bGwpIHtcbiAgICAgIHRoaXMuc3RlcHMuZm9yRWFjaChzdGVwID0+IHtcbiAgICAgICAgY29uc3QgdmFsdWUgPSBzdGVwLnZhbHVlO1xuICAgICAgICBjb25zdCBpc0FjdGl2ZSA9XG4gICAgICAgICAgKCF0aGlzLm56SW5jbHVkZWQgJiYgdmFsdWUgPT09IHRoaXMubnpVcHBlckJvdW5kKSB8fFxuICAgICAgICAgICh0aGlzLm56SW5jbHVkZWQgJiYgdmFsdWUgPD0gdGhpcy5uelVwcGVyQm91bmQhICYmIHZhbHVlID49IHRoaXMubnpMb3dlckJvdW5kISk7XG4gICAgICAgIHN0ZXAuYWN0aXZlID0gaXNBY3RpdmU7XG4gICAgICB9KTtcbiAgICB9XG4gIH1cbn1cbiJdfQ==