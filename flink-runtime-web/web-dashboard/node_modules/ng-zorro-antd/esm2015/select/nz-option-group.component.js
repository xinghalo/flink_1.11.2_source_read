/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { ChangeDetectionStrategy, Component, ContentChildren, Input, QueryList, TemplateRef, ViewEncapsulation } from '@angular/core';
import { NzOptionComponent } from './nz-option.component';
export class NzOptionGroupComponent {
    constructor() {
        this.isLabelString = false;
    }
    /**
     * @param {?} value
     * @return {?}
     */
    set nzLabel(value) {
        this.label = value;
        this.isLabelString = !(this.nzLabel instanceof TemplateRef);
    }
    /**
     * @return {?}
     */
    get nzLabel() {
        return this.label;
    }
}
NzOptionGroupComponent.decorators = [
    { type: Component, args: [{
                selector: 'nz-option-group',
                encapsulation: ViewEncapsulation.None,
                changeDetection: ChangeDetectionStrategy.OnPush,
                template: "<ng-content></ng-content>"
            }] }
];
NzOptionGroupComponent.propDecorators = {
    listOfNzOptionComponent: [{ type: ContentChildren, args: [NzOptionComponent,] }],
    nzLabel: [{ type: Input }]
};
if (false) {
    /** @type {?} */
    NzOptionGroupComponent.prototype.isLabelString;
    /** @type {?} */
    NzOptionGroupComponent.prototype.label;
    /** @type {?} */
    NzOptionGroupComponent.prototype.listOfNzOptionComponent;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotb3B0aW9uLWdyb3VwLmNvbXBvbmVudC5qcyIsInNvdXJjZVJvb3QiOiJuZzovL25nLXpvcnJvLWFudGQvIiwic291cmNlcyI6WyJzZWxlY3Qvbnotb3B0aW9uLWdyb3VwLmNvbXBvbmVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7O0FBQUEsT0FBTyxFQUNMLHVCQUF1QixFQUN2QixTQUFTLEVBQ1QsZUFBZSxFQUNmLEtBQUssRUFDTCxTQUFTLEVBQ1QsV0FBVyxFQUNYLGlCQUFpQixFQUNsQixNQUFNLGVBQWUsQ0FBQztBQUN2QixPQUFPLEVBQUUsaUJBQWlCLEVBQUUsTUFBTSx1QkFBdUIsQ0FBQztBQVExRCxNQUFNLE9BQU8sc0JBQXNCO0lBTm5DO1FBT0Usa0JBQWEsR0FBRyxLQUFLLENBQUM7SUFheEIsQ0FBQzs7Ozs7SUFUQyxJQUNJLE9BQU8sQ0FBQyxLQUFpQztRQUMzQyxJQUFJLENBQUMsS0FBSyxHQUFHLEtBQUssQ0FBQztRQUNuQixJQUFJLENBQUMsYUFBYSxHQUFHLENBQUMsQ0FBQyxJQUFJLENBQUMsT0FBTyxZQUFZLFdBQVcsQ0FBQyxDQUFDO0lBQzlELENBQUM7Ozs7SUFFRCxJQUFJLE9BQU87UUFDVCxPQUFPLElBQUksQ0FBQyxLQUFLLENBQUM7SUFDcEIsQ0FBQzs7O1lBbkJGLFNBQVMsU0FBQztnQkFDVCxRQUFRLEVBQUUsaUJBQWlCO2dCQUMzQixhQUFhLEVBQUUsaUJBQWlCLENBQUMsSUFBSTtnQkFDckMsZUFBZSxFQUFFLHVCQUF1QixDQUFDLE1BQU07Z0JBQy9DLHFDQUErQzthQUNoRDs7O3NDQUlFLGVBQWUsU0FBQyxpQkFBaUI7c0JBRWpDLEtBQUs7Ozs7SUFKTiwrQ0FBc0I7O0lBQ3RCLHVDQUFrQzs7SUFDbEMseURBQTBGIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHtcbiAgQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3ksXG4gIENvbXBvbmVudCxcbiAgQ29udGVudENoaWxkcmVuLFxuICBJbnB1dCxcbiAgUXVlcnlMaXN0LFxuICBUZW1wbGF0ZVJlZixcbiAgVmlld0VuY2Fwc3VsYXRpb25cbn0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5pbXBvcnQgeyBOek9wdGlvbkNvbXBvbmVudCB9IGZyb20gJy4vbnotb3B0aW9uLmNvbXBvbmVudCc7XG5cbkBDb21wb25lbnQoe1xuICBzZWxlY3RvcjogJ256LW9wdGlvbi1ncm91cCcsXG4gIGVuY2Fwc3VsYXRpb246IFZpZXdFbmNhcHN1bGF0aW9uLk5vbmUsXG4gIGNoYW5nZURldGVjdGlvbjogQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3kuT25QdXNoLFxuICB0ZW1wbGF0ZVVybDogJy4vbnotb3B0aW9uLWdyb3VwLmNvbXBvbmVudC5odG1sJ1xufSlcbmV4cG9ydCBjbGFzcyBOek9wdGlvbkdyb3VwQ29tcG9uZW50IHtcbiAgaXNMYWJlbFN0cmluZyA9IGZhbHNlO1xuICBsYWJlbDogc3RyaW5nIHwgVGVtcGxhdGVSZWY8dm9pZD47XG4gIEBDb250ZW50Q2hpbGRyZW4oTnpPcHRpb25Db21wb25lbnQpIGxpc3RPZk56T3B0aW9uQ29tcG9uZW50OiBRdWVyeUxpc3Q8TnpPcHRpb25Db21wb25lbnQ+O1xuXG4gIEBJbnB1dCgpXG4gIHNldCBuekxhYmVsKHZhbHVlOiBzdHJpbmcgfCBUZW1wbGF0ZVJlZjx2b2lkPikge1xuICAgIHRoaXMubGFiZWwgPSB2YWx1ZTtcbiAgICB0aGlzLmlzTGFiZWxTdHJpbmcgPSAhKHRoaXMubnpMYWJlbCBpbnN0YW5jZW9mIFRlbXBsYXRlUmVmKTtcbiAgfVxuXG4gIGdldCBuekxhYmVsKCk6IHN0cmluZyB8IFRlbXBsYXRlUmVmPHZvaWQ+IHtcbiAgICByZXR1cm4gdGhpcy5sYWJlbDtcbiAgfVxufVxuIl19