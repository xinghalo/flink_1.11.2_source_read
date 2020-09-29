/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import * as tslib_1 from "tslib";
import { ChangeDetectionStrategy, Component, ElementRef, Input, Renderer2, TemplateRef, ViewChild, ViewEncapsulation } from '@angular/core';
import { zoomBadgeMotion } from '../core/animation/zoom';
import { isEmpty } from '../core/util/check';
import { InputBoolean } from '../core/util/convert';
export class NzBadgeComponent {
    /**
     * @param {?} renderer
     * @param {?} elementRef
     */
    constructor(renderer, elementRef) {
        this.renderer = renderer;
        this.elementRef = elementRef;
        this.maxNumberArray = [];
        this.countArray = [];
        this.countSingleArray = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        this.nzShowZero = false;
        this.nzShowDot = true;
        this.nzDot = false;
        this.nzOverflowCount = 99;
        renderer.addClass(elementRef.nativeElement, 'ant-badge');
    }
    /**
     * @return {?}
     */
    checkContent() {
        if (isEmpty(this.contentElement.nativeElement)) {
            this.renderer.addClass(this.elementRef.nativeElement, 'ant-badge-not-a-wrapper');
        }
        else {
            this.renderer.removeClass(this.elementRef.nativeElement, 'ant-badge-not-a-wrapper');
        }
    }
    /**
     * @return {?}
     */
    get showSup() {
        return (this.nzShowDot && this.nzDot) || this.count > 0 || (this.count === 0 && this.nzShowZero);
    }
    /**
     * @return {?}
     */
    generateMaxNumberArray() {
        this.maxNumberArray = this.nzOverflowCount.toString().split('');
    }
    /**
     * @return {?}
     */
    ngOnInit() {
        this.generateMaxNumberArray();
    }
    /**
     * @return {?}
     */
    ngAfterViewInit() {
        this.checkContent();
    }
    /**
     * @param {?} changes
     * @return {?}
     */
    ngOnChanges(changes) {
        const { nzOverflowCount, nzCount } = changes;
        if (nzCount && !(nzCount.currentValue instanceof TemplateRef)) {
            this.count = Math.max(0, nzCount.currentValue);
            this.countArray = this.count
                .toString()
                .split('')
                .map((/**
             * @param {?} item
             * @return {?}
             */
            item => +item));
        }
        if (nzOverflowCount) {
            this.generateMaxNumberArray();
        }
    }
}
NzBadgeComponent.decorators = [
    { type: Component, args: [{
                selector: 'nz-badge',
                preserveWhitespaces: false,
                encapsulation: ViewEncapsulation.None,
                changeDetection: ChangeDetectionStrategy.OnPush,
                animations: [zoomBadgeMotion],
                template: "<span (cdkObserveContent)=\"checkContent()\" #contentElement><ng-content></ng-content></span>\n<span class=\"ant-badge-status-dot ant-badge-status-{{nzStatus}}\" *ngIf=\"nzStatus\" [ngStyle]=\"nzStyle\"></span>\n<span class=\"ant-badge-status-text\" *ngIf=\"nzStatus\">{{ nzText }}</span>\n<ng-container *nzStringTemplateOutlet=\"nzCount\">\n  <sup class=\"ant-scroll-number\"\n    *ngIf=\"showSup\"\n    @zoomBadgeMotion\n    [ngStyle]=\"nzStyle\"\n    [class.ant-badge-count]=\"!nzDot\"\n    [class.ant-badge-dot]=\"nzDot\"\n    [class.ant-badge-multiple-words]=\"countArray.length>=2\">\n    <ng-container *ngFor=\"let n of maxNumberArray;let i = index;\">\n      <span class=\"ant-scroll-number-only\"\n        *ngIf=\"count <= nzOverflowCount\"\n        [style.transform]=\"'translateY(' + (-countArray[i] * 100) + '%)'\">\n          <ng-container *ngIf=\"(!nzDot)&&(countArray[i]!=null)\">\n            <p *ngFor=\"let p of countSingleArray\" [class.current]=\"p === countArray[i]\">{{ p }}</p>\n          </ng-container>\n      </span>\n    </ng-container>\n    <ng-container *ngIf=\"count > nzOverflowCount\">{{ nzOverflowCount }}+</ng-container>\n  </sup>\n</ng-container>",
                host: {
                    '[class.ant-badge-status]': 'nzStatus'
                }
            }] }
];
/** @nocollapse */
NzBadgeComponent.ctorParameters = () => [
    { type: Renderer2 },
    { type: ElementRef }
];
NzBadgeComponent.propDecorators = {
    contentElement: [{ type: ViewChild, args: ['contentElement',] }],
    nzShowZero: [{ type: Input }],
    nzShowDot: [{ type: Input }],
    nzDot: [{ type: Input }],
    nzOverflowCount: [{ type: Input }],
    nzText: [{ type: Input }],
    nzStyle: [{ type: Input }],
    nzStatus: [{ type: Input }],
    nzCount: [{ type: Input }]
};
tslib_1.__decorate([
    InputBoolean(),
    tslib_1.__metadata("design:type", Object)
], NzBadgeComponent.prototype, "nzShowZero", void 0);
tslib_1.__decorate([
    InputBoolean(),
    tslib_1.__metadata("design:type", Object)
], NzBadgeComponent.prototype, "nzShowDot", void 0);
tslib_1.__decorate([
    InputBoolean(),
    tslib_1.__metadata("design:type", Object)
], NzBadgeComponent.prototype, "nzDot", void 0);
if (false) {
    /** @type {?} */
    NzBadgeComponent.prototype.maxNumberArray;
    /** @type {?} */
    NzBadgeComponent.prototype.countArray;
    /** @type {?} */
    NzBadgeComponent.prototype.countSingleArray;
    /** @type {?} */
    NzBadgeComponent.prototype.count;
    /** @type {?} */
    NzBadgeComponent.prototype.contentElement;
    /** @type {?} */
    NzBadgeComponent.prototype.nzShowZero;
    /** @type {?} */
    NzBadgeComponent.prototype.nzShowDot;
    /** @type {?} */
    NzBadgeComponent.prototype.nzDot;
    /** @type {?} */
    NzBadgeComponent.prototype.nzOverflowCount;
    /** @type {?} */
    NzBadgeComponent.prototype.nzText;
    /** @type {?} */
    NzBadgeComponent.prototype.nzStyle;
    /** @type {?} */
    NzBadgeComponent.prototype.nzStatus;
    /** @type {?} */
    NzBadgeComponent.prototype.nzCount;
    /**
     * @type {?}
     * @private
     */
    NzBadgeComponent.prototype.renderer;
    /**
     * @type {?}
     * @private
     */
    NzBadgeComponent.prototype.elementRef;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotYmFkZ2UuY29tcG9uZW50LmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbImJhZGdlL256LWJhZGdlLmNvbXBvbmVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7OztBQUFBLE9BQU8sRUFFTCx1QkFBdUIsRUFDdkIsU0FBUyxFQUNULFVBQVUsRUFDVixLQUFLLEVBR0wsU0FBUyxFQUVULFdBQVcsRUFDWCxTQUFTLEVBQ1QsaUJBQWlCLEVBQ2xCLE1BQU0sZUFBZSxDQUFDO0FBQ3ZCLE9BQU8sRUFBRSxlQUFlLEVBQUUsTUFBTSx3QkFBd0IsQ0FBQztBQUN6RCxPQUFPLEVBQUUsT0FBTyxFQUFFLE1BQU0sb0JBQW9CLENBQUM7QUFDN0MsT0FBTyxFQUFFLFlBQVksRUFBRSxNQUFNLHNCQUFzQixDQUFDO0FBZXBELE1BQU0sT0FBTyxnQkFBZ0I7Ozs7O0lBK0IzQixZQUFvQixRQUFtQixFQUFVLFVBQXNCO1FBQW5ELGFBQVEsR0FBUixRQUFRLENBQVc7UUFBVSxlQUFVLEdBQVYsVUFBVSxDQUFZO1FBOUJ2RSxtQkFBYyxHQUFhLEVBQUUsQ0FBQztRQUM5QixlQUFVLEdBQWEsRUFBRSxDQUFDO1FBQzFCLHFCQUFnQixHQUFHLENBQUMsQ0FBQyxFQUFFLENBQUMsRUFBRSxDQUFDLEVBQUUsQ0FBQyxFQUFFLENBQUMsRUFBRSxDQUFDLEVBQUUsQ0FBQyxFQUFFLENBQUMsRUFBRSxDQUFDLEVBQUUsQ0FBQyxDQUFDLENBQUM7UUFHekIsZUFBVSxHQUFHLEtBQUssQ0FBQztRQUNuQixjQUFTLEdBQUcsSUFBSSxDQUFDO1FBQ2pCLFVBQUssR0FBRyxLQUFLLENBQUM7UUFDOUIsb0JBQWUsR0FBRyxFQUFFLENBQUM7UUF1QjVCLFFBQVEsQ0FBQyxRQUFRLENBQUMsVUFBVSxDQUFDLGFBQWEsRUFBRSxXQUFXLENBQUMsQ0FBQztJQUMzRCxDQUFDOzs7O0lBbEJELFlBQVk7UUFDVixJQUFJLE9BQU8sQ0FBQyxJQUFJLENBQUMsY0FBYyxDQUFDLGFBQWEsQ0FBQyxFQUFFO1lBQzlDLElBQUksQ0FBQyxRQUFRLENBQUMsUUFBUSxDQUFDLElBQUksQ0FBQyxVQUFVLENBQUMsYUFBYSxFQUFFLHlCQUF5QixDQUFDLENBQUM7U0FDbEY7YUFBTTtZQUNMLElBQUksQ0FBQyxRQUFRLENBQUMsV0FBVyxDQUFDLElBQUksQ0FBQyxVQUFVLENBQUMsYUFBYSxFQUFFLHlCQUF5QixDQUFDLENBQUM7U0FDckY7SUFDSCxDQUFDOzs7O0lBRUQsSUFBSSxPQUFPO1FBQ1QsT0FBTyxDQUFDLElBQUksQ0FBQyxTQUFTLElBQUksSUFBSSxDQUFDLEtBQUssQ0FBQyxJQUFJLElBQUksQ0FBQyxLQUFLLEdBQUcsQ0FBQyxJQUFJLENBQUMsSUFBSSxDQUFDLEtBQUssS0FBSyxDQUFDLElBQUksSUFBSSxDQUFDLFVBQVUsQ0FBQyxDQUFDO0lBQ25HLENBQUM7Ozs7SUFFRCxzQkFBc0I7UUFDcEIsSUFBSSxDQUFDLGNBQWMsR0FBRyxJQUFJLENBQUMsZUFBZSxDQUFDLFFBQVEsRUFBRSxDQUFDLEtBQUssQ0FBQyxFQUFFLENBQUMsQ0FBQztJQUNsRSxDQUFDOzs7O0lBTUQsUUFBUTtRQUNOLElBQUksQ0FBQyxzQkFBc0IsRUFBRSxDQUFDO0lBQ2hDLENBQUM7Ozs7SUFFRCxlQUFlO1FBQ2IsSUFBSSxDQUFDLFlBQVksRUFBRSxDQUFDO0lBQ3RCLENBQUM7Ozs7O0lBRUQsV0FBVyxDQUFDLE9BQXNCO2NBQzFCLEVBQUUsZUFBZSxFQUFFLE9BQU8sRUFBRSxHQUFHLE9BQU87UUFDNUMsSUFBSSxPQUFPLElBQUksQ0FBQyxDQUFDLE9BQU8sQ0FBQyxZQUFZLFlBQVksV0FBVyxDQUFDLEVBQUU7WUFDN0QsSUFBSSxDQUFDLEtBQUssR0FBRyxJQUFJLENBQUMsR0FBRyxDQUFDLENBQUMsRUFBRSxPQUFPLENBQUMsWUFBWSxDQUFDLENBQUM7WUFDL0MsSUFBSSxDQUFDLFVBQVUsR0FBRyxJQUFJLENBQUMsS0FBSztpQkFDekIsUUFBUSxFQUFFO2lCQUNWLEtBQUssQ0FBQyxFQUFFLENBQUM7aUJBQ1QsR0FBRzs7OztZQUFDLElBQUksQ0FBQyxFQUFFLENBQUMsQ0FBQyxJQUFJLEVBQUMsQ0FBQztTQUN2QjtRQUNELElBQUksZUFBZSxFQUFFO1lBQ25CLElBQUksQ0FBQyxzQkFBc0IsRUFBRSxDQUFDO1NBQy9CO0lBQ0gsQ0FBQzs7O1lBbEVGLFNBQVMsU0FBQztnQkFDVCxRQUFRLEVBQUUsVUFBVTtnQkFDcEIsbUJBQW1CLEVBQUUsS0FBSztnQkFDMUIsYUFBYSxFQUFFLGlCQUFpQixDQUFDLElBQUk7Z0JBQ3JDLGVBQWUsRUFBRSx1QkFBdUIsQ0FBQyxNQUFNO2dCQUMvQyxVQUFVLEVBQUUsQ0FBQyxlQUFlLENBQUM7Z0JBQzdCLHlxQ0FBd0M7Z0JBQ3hDLElBQUksRUFBRTtvQkFDSiwwQkFBMEIsRUFBRSxVQUFVO2lCQUN2QzthQUNGOzs7O1lBdEJDLFNBQVM7WUFKVCxVQUFVOzs7NkJBZ0NULFNBQVMsU0FBQyxnQkFBZ0I7eUJBQzFCLEtBQUs7d0JBQ0wsS0FBSztvQkFDTCxLQUFLOzhCQUNMLEtBQUs7cUJBQ0wsS0FBSztzQkFDTCxLQUFLO3VCQUNMLEtBQUs7c0JBQ0wsS0FBSzs7QUFQbUI7SUFBZixZQUFZLEVBQUU7O29EQUFvQjtBQUNuQjtJQUFmLFlBQVksRUFBRTs7bURBQWtCO0FBQ2pCO0lBQWYsWUFBWSxFQUFFOzsrQ0FBZTs7O0lBUHZDLDBDQUE4Qjs7SUFDOUIsc0NBQTBCOztJQUMxQiw0Q0FBa0Q7O0lBQ2xELGlDQUFjOztJQUNkLDBDQUF3RDs7SUFDeEQsc0NBQTRDOztJQUM1QyxxQ0FBMEM7O0lBQzFDLGlDQUF1Qzs7SUFDdkMsMkNBQThCOztJQUM5QixrQ0FBd0I7O0lBQ3hCLG1DQUE0Qzs7SUFDNUMsb0NBQXFDOztJQUNyQyxtQ0FBNkM7Ozs7O0lBa0JqQyxvQ0FBMkI7Ozs7O0lBQUUsc0NBQThCIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHtcbiAgQWZ0ZXJWaWV3SW5pdCxcbiAgQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3ksXG4gIENvbXBvbmVudCxcbiAgRWxlbWVudFJlZixcbiAgSW5wdXQsXG4gIE9uQ2hhbmdlcyxcbiAgT25Jbml0LFxuICBSZW5kZXJlcjIsXG4gIFNpbXBsZUNoYW5nZXMsXG4gIFRlbXBsYXRlUmVmLFxuICBWaWV3Q2hpbGQsXG4gIFZpZXdFbmNhcHN1bGF0aW9uXG59IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuaW1wb3J0IHsgem9vbUJhZGdlTW90aW9uIH0gZnJvbSAnLi4vY29yZS9hbmltYXRpb24vem9vbSc7XG5pbXBvcnQgeyBpc0VtcHR5IH0gZnJvbSAnLi4vY29yZS91dGlsL2NoZWNrJztcbmltcG9ydCB7IElucHV0Qm9vbGVhbiB9IGZyb20gJy4uL2NvcmUvdXRpbC9jb252ZXJ0JztcblxuZXhwb3J0IHR5cGUgTnpCYWRnZVN0YXR1c1R5cGUgPSAnc3VjY2VzcycgfCAncHJvY2Vzc2luZycgfCAnZGVmYXVsdCcgfCAnZXJyb3InIHwgJ3dhcm5pbmcnO1xuXG5AQ29tcG9uZW50KHtcbiAgc2VsZWN0b3I6ICduei1iYWRnZScsXG4gIHByZXNlcnZlV2hpdGVzcGFjZXM6IGZhbHNlLFxuICBlbmNhcHN1bGF0aW9uOiBWaWV3RW5jYXBzdWxhdGlvbi5Ob25lLFxuICBjaGFuZ2VEZXRlY3Rpb246IENoYW5nZURldGVjdGlvblN0cmF0ZWd5Lk9uUHVzaCxcbiAgYW5pbWF0aW9uczogW3pvb21CYWRnZU1vdGlvbl0sXG4gIHRlbXBsYXRlVXJsOiAnLi9uei1iYWRnZS5jb21wb25lbnQuaHRtbCcsXG4gIGhvc3Q6IHtcbiAgICAnW2NsYXNzLmFudC1iYWRnZS1zdGF0dXNdJzogJ256U3RhdHVzJ1xuICB9XG59KVxuZXhwb3J0IGNsYXNzIE56QmFkZ2VDb21wb25lbnQgaW1wbGVtZW50cyBPbkluaXQsIEFmdGVyVmlld0luaXQsIE9uQ2hhbmdlcyB7XG4gIG1heE51bWJlckFycmF5OiBzdHJpbmdbXSA9IFtdO1xuICBjb3VudEFycmF5OiBudW1iZXJbXSA9IFtdO1xuICBjb3VudFNpbmdsZUFycmF5ID0gWzAsIDEsIDIsIDMsIDQsIDUsIDYsIDcsIDgsIDldO1xuICBjb3VudDogbnVtYmVyO1xuICBAVmlld0NoaWxkKCdjb250ZW50RWxlbWVudCcpIGNvbnRlbnRFbGVtZW50OiBFbGVtZW50UmVmO1xuICBASW5wdXQoKSBASW5wdXRCb29sZWFuKCkgbnpTaG93WmVybyA9IGZhbHNlO1xuICBASW5wdXQoKSBASW5wdXRCb29sZWFuKCkgbnpTaG93RG90ID0gdHJ1ZTtcbiAgQElucHV0KCkgQElucHV0Qm9vbGVhbigpIG56RG90ID0gZmFsc2U7XG4gIEBJbnB1dCgpIG56T3ZlcmZsb3dDb3VudCA9IDk5O1xuICBASW5wdXQoKSBuelRleHQ6IHN0cmluZztcbiAgQElucHV0KCkgbnpTdHlsZTogeyBba2V5OiBzdHJpbmddOiBzdHJpbmcgfTtcbiAgQElucHV0KCkgbnpTdGF0dXM6IE56QmFkZ2VTdGF0dXNUeXBlO1xuICBASW5wdXQoKSBuekNvdW50OiBudW1iZXIgfCBUZW1wbGF0ZVJlZjx2b2lkPjtcblxuICBjaGVja0NvbnRlbnQoKTogdm9pZCB7XG4gICAgaWYgKGlzRW1wdHkodGhpcy5jb250ZW50RWxlbWVudC5uYXRpdmVFbGVtZW50KSkge1xuICAgICAgdGhpcy5yZW5kZXJlci5hZGRDbGFzcyh0aGlzLmVsZW1lbnRSZWYubmF0aXZlRWxlbWVudCwgJ2FudC1iYWRnZS1ub3QtYS13cmFwcGVyJyk7XG4gICAgfSBlbHNlIHtcbiAgICAgIHRoaXMucmVuZGVyZXIucmVtb3ZlQ2xhc3ModGhpcy5lbGVtZW50UmVmLm5hdGl2ZUVsZW1lbnQsICdhbnQtYmFkZ2Utbm90LWEtd3JhcHBlcicpO1xuICAgIH1cbiAgfVxuXG4gIGdldCBzaG93U3VwKCk6IGJvb2xlYW4ge1xuICAgIHJldHVybiAodGhpcy5uelNob3dEb3QgJiYgdGhpcy5uekRvdCkgfHwgdGhpcy5jb3VudCA+IDAgfHwgKHRoaXMuY291bnQgPT09IDAgJiYgdGhpcy5uelNob3daZXJvKTtcbiAgfVxuXG4gIGdlbmVyYXRlTWF4TnVtYmVyQXJyYXkoKTogdm9pZCB7XG4gICAgdGhpcy5tYXhOdW1iZXJBcnJheSA9IHRoaXMubnpPdmVyZmxvd0NvdW50LnRvU3RyaW5nKCkuc3BsaXQoJycpO1xuICB9XG5cbiAgY29uc3RydWN0b3IocHJpdmF0ZSByZW5kZXJlcjogUmVuZGVyZXIyLCBwcml2YXRlIGVsZW1lbnRSZWY6IEVsZW1lbnRSZWYpIHtcbiAgICByZW5kZXJlci5hZGRDbGFzcyhlbGVtZW50UmVmLm5hdGl2ZUVsZW1lbnQsICdhbnQtYmFkZ2UnKTtcbiAgfVxuXG4gIG5nT25Jbml0KCk6IHZvaWQge1xuICAgIHRoaXMuZ2VuZXJhdGVNYXhOdW1iZXJBcnJheSgpO1xuICB9XG5cbiAgbmdBZnRlclZpZXdJbml0KCk6IHZvaWQge1xuICAgIHRoaXMuY2hlY2tDb250ZW50KCk7XG4gIH1cblxuICBuZ09uQ2hhbmdlcyhjaGFuZ2VzOiBTaW1wbGVDaGFuZ2VzKTogdm9pZCB7XG4gICAgY29uc3QgeyBuek92ZXJmbG93Q291bnQsIG56Q291bnQgfSA9IGNoYW5nZXM7XG4gICAgaWYgKG56Q291bnQgJiYgIShuekNvdW50LmN1cnJlbnRWYWx1ZSBpbnN0YW5jZW9mIFRlbXBsYXRlUmVmKSkge1xuICAgICAgdGhpcy5jb3VudCA9IE1hdGgubWF4KDAsIG56Q291bnQuY3VycmVudFZhbHVlKTtcbiAgICAgIHRoaXMuY291bnRBcnJheSA9IHRoaXMuY291bnRcbiAgICAgICAgLnRvU3RyaW5nKClcbiAgICAgICAgLnNwbGl0KCcnKVxuICAgICAgICAubWFwKGl0ZW0gPT4gK2l0ZW0pO1xuICAgIH1cbiAgICBpZiAobnpPdmVyZmxvd0NvdW50KSB7XG4gICAgICB0aGlzLmdlbmVyYXRlTWF4TnVtYmVyQXJyYXkoKTtcbiAgICB9XG4gIH1cbn1cbiJdfQ==