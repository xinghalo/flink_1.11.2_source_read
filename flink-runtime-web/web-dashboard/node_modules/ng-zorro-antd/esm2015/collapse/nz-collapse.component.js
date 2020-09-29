/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import * as tslib_1 from "tslib";
import { ChangeDetectionStrategy, Component, Input, ViewEncapsulation } from '@angular/core';
import { InputBoolean } from '../core/util/convert';
export class NzCollapseComponent {
    constructor() {
        this.listOfNzCollapsePanelComponent = [];
        this.nzAccordion = false;
        this.nzBordered = true;
    }
    /**
     * @param {?} value
     * @return {?}
     */
    addPanel(value) {
        this.listOfNzCollapsePanelComponent.push(value);
    }
    /**
     * @param {?} value
     * @return {?}
     */
    removePanel(value) {
        this.listOfNzCollapsePanelComponent.splice(this.listOfNzCollapsePanelComponent.indexOf(value), 1);
    }
    /**
     * @param {?} collapse
     * @return {?}
     */
    click(collapse) {
        if (this.nzAccordion && !collapse.nzActive) {
            this.listOfNzCollapsePanelComponent
                .filter((/**
             * @param {?} item
             * @return {?}
             */
            item => item !== collapse))
                .forEach((/**
             * @param {?} item
             * @return {?}
             */
            item => {
                if (item.nzActive) {
                    item.nzActive = false;
                    item.nzActiveChange.emit(item.nzActive);
                    item.markForCheck();
                }
            }));
        }
        collapse.nzActive = !collapse.nzActive;
        collapse.nzActiveChange.emit(collapse.nzActive);
    }
}
NzCollapseComponent.decorators = [
    { type: Component, args: [{
                selector: 'nz-collapse',
                template: "<div class=\"ant-collapse\" [class.ant-collapse-borderless]=\"!nzBordered\">\n  <ng-content></ng-content>\n</div>",
                changeDetection: ChangeDetectionStrategy.OnPush,
                encapsulation: ViewEncapsulation.None,
                styles: [`
      nz-collapse {
        display: block;
      }
    `]
            }] }
];
NzCollapseComponent.propDecorators = {
    nzAccordion: [{ type: Input }],
    nzBordered: [{ type: Input }]
};
tslib_1.__decorate([
    InputBoolean(),
    tslib_1.__metadata("design:type", Object)
], NzCollapseComponent.prototype, "nzAccordion", void 0);
tslib_1.__decorate([
    InputBoolean(),
    tslib_1.__metadata("design:type", Object)
], NzCollapseComponent.prototype, "nzBordered", void 0);
if (false) {
    /**
     * @type {?}
     * @private
     */
    NzCollapseComponent.prototype.listOfNzCollapsePanelComponent;
    /** @type {?} */
    NzCollapseComponent.prototype.nzAccordion;
    /** @type {?} */
    NzCollapseComponent.prototype.nzBordered;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotY29sbGFwc2UuY29tcG9uZW50LmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbImNvbGxhcHNlL256LWNvbGxhcHNlLmNvbXBvbmVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7OztBQUFBLE9BQU8sRUFBRSx1QkFBdUIsRUFBRSxTQUFTLEVBQUUsS0FBSyxFQUFFLGlCQUFpQixFQUFFLE1BQU0sZUFBZSxDQUFDO0FBQzdGLE9BQU8sRUFBRSxZQUFZLEVBQUUsTUFBTSxzQkFBc0IsQ0FBQztBQWdCcEQsTUFBTSxPQUFPLG1CQUFtQjtJQWJoQztRQWNVLG1DQUE4QixHQUErQixFQUFFLENBQUM7UUFDL0MsZ0JBQVcsR0FBRyxLQUFLLENBQUM7UUFDcEIsZUFBVSxHQUFHLElBQUksQ0FBQztJQXlCN0MsQ0FBQzs7Ozs7SUF2QkMsUUFBUSxDQUFDLEtBQStCO1FBQ3RDLElBQUksQ0FBQyw4QkFBOEIsQ0FBQyxJQUFJLENBQUMsS0FBSyxDQUFDLENBQUM7SUFDbEQsQ0FBQzs7Ozs7SUFFRCxXQUFXLENBQUMsS0FBK0I7UUFDekMsSUFBSSxDQUFDLDhCQUE4QixDQUFDLE1BQU0sQ0FBQyxJQUFJLENBQUMsOEJBQThCLENBQUMsT0FBTyxDQUFDLEtBQUssQ0FBQyxFQUFFLENBQUMsQ0FBQyxDQUFDO0lBQ3BHLENBQUM7Ozs7O0lBRUQsS0FBSyxDQUFDLFFBQWtDO1FBQ3RDLElBQUksSUFBSSxDQUFDLFdBQVcsSUFBSSxDQUFDLFFBQVEsQ0FBQyxRQUFRLEVBQUU7WUFDMUMsSUFBSSxDQUFDLDhCQUE4QjtpQkFDaEMsTUFBTTs7OztZQUFDLElBQUksQ0FBQyxFQUFFLENBQUMsSUFBSSxLQUFLLFFBQVEsRUFBQztpQkFDakMsT0FBTzs7OztZQUFDLElBQUksQ0FBQyxFQUFFO2dCQUNkLElBQUksSUFBSSxDQUFDLFFBQVEsRUFBRTtvQkFDakIsSUFBSSxDQUFDLFFBQVEsR0FBRyxLQUFLLENBQUM7b0JBQ3RCLElBQUksQ0FBQyxjQUFjLENBQUMsSUFBSSxDQUFDLElBQUksQ0FBQyxRQUFRLENBQUMsQ0FBQztvQkFDeEMsSUFBSSxDQUFDLFlBQVksRUFBRSxDQUFDO2lCQUNyQjtZQUNILENBQUMsRUFBQyxDQUFDO1NBQ047UUFDRCxRQUFRLENBQUMsUUFBUSxHQUFHLENBQUMsUUFBUSxDQUFDLFFBQVEsQ0FBQztRQUN2QyxRQUFRLENBQUMsY0FBYyxDQUFDLElBQUksQ0FBQyxRQUFRLENBQUMsUUFBUSxDQUFDLENBQUM7SUFDbEQsQ0FBQzs7O1lBeENGLFNBQVMsU0FBQztnQkFDVCxRQUFRLEVBQUUsYUFBYTtnQkFDdkIsNkhBQTJDO2dCQUMzQyxlQUFlLEVBQUUsdUJBQXVCLENBQUMsTUFBTTtnQkFDL0MsYUFBYSxFQUFFLGlCQUFpQixDQUFDLElBQUk7eUJBRW5DOzs7O0tBSUM7YUFFSjs7OzBCQUdFLEtBQUs7eUJBQ0wsS0FBSzs7QUFEbUI7SUFBZixZQUFZLEVBQUU7O3dEQUFxQjtBQUNwQjtJQUFmLFlBQVksRUFBRTs7dURBQW1COzs7Ozs7SUFGM0MsNkRBQXdFOztJQUN4RSwwQ0FBNkM7O0lBQzdDLHlDQUEyQyIsInNvdXJjZXNDb250ZW50IjpbImltcG9ydCB7IENoYW5nZURldGVjdGlvblN0cmF0ZWd5LCBDb21wb25lbnQsIElucHV0LCBWaWV3RW5jYXBzdWxhdGlvbiB9IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuaW1wb3J0IHsgSW5wdXRCb29sZWFuIH0gZnJvbSAnLi4vY29yZS91dGlsL2NvbnZlcnQnO1xuaW1wb3J0IHsgTnpDb2xsYXBzZVBhbmVsQ29tcG9uZW50IH0gZnJvbSAnLi9uei1jb2xsYXBzZS1wYW5lbC5jb21wb25lbnQnO1xuXG5AQ29tcG9uZW50KHtcbiAgc2VsZWN0b3I6ICduei1jb2xsYXBzZScsXG4gIHRlbXBsYXRlVXJsOiAnLi9uei1jb2xsYXBzZS5jb21wb25lbnQuaHRtbCcsXG4gIGNoYW5nZURldGVjdGlvbjogQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3kuT25QdXNoLFxuICBlbmNhcHN1bGF0aW9uOiBWaWV3RW5jYXBzdWxhdGlvbi5Ob25lLFxuICBzdHlsZXM6IFtcbiAgICBgXG4gICAgICBuei1jb2xsYXBzZSB7XG4gICAgICAgIGRpc3BsYXk6IGJsb2NrO1xuICAgICAgfVxuICAgIGBcbiAgXVxufSlcbmV4cG9ydCBjbGFzcyBOekNvbGxhcHNlQ29tcG9uZW50IHtcbiAgcHJpdmF0ZSBsaXN0T2ZOekNvbGxhcHNlUGFuZWxDb21wb25lbnQ6IE56Q29sbGFwc2VQYW5lbENvbXBvbmVudFtdID0gW107XG4gIEBJbnB1dCgpIEBJbnB1dEJvb2xlYW4oKSBuekFjY29yZGlvbiA9IGZhbHNlO1xuICBASW5wdXQoKSBASW5wdXRCb29sZWFuKCkgbnpCb3JkZXJlZCA9IHRydWU7XG5cbiAgYWRkUGFuZWwodmFsdWU6IE56Q29sbGFwc2VQYW5lbENvbXBvbmVudCk6IHZvaWQge1xuICAgIHRoaXMubGlzdE9mTnpDb2xsYXBzZVBhbmVsQ29tcG9uZW50LnB1c2godmFsdWUpO1xuICB9XG5cbiAgcmVtb3ZlUGFuZWwodmFsdWU6IE56Q29sbGFwc2VQYW5lbENvbXBvbmVudCk6IHZvaWQge1xuICAgIHRoaXMubGlzdE9mTnpDb2xsYXBzZVBhbmVsQ29tcG9uZW50LnNwbGljZSh0aGlzLmxpc3RPZk56Q29sbGFwc2VQYW5lbENvbXBvbmVudC5pbmRleE9mKHZhbHVlKSwgMSk7XG4gIH1cblxuICBjbGljayhjb2xsYXBzZTogTnpDb2xsYXBzZVBhbmVsQ29tcG9uZW50KTogdm9pZCB7XG4gICAgaWYgKHRoaXMubnpBY2NvcmRpb24gJiYgIWNvbGxhcHNlLm56QWN0aXZlKSB7XG4gICAgICB0aGlzLmxpc3RPZk56Q29sbGFwc2VQYW5lbENvbXBvbmVudFxuICAgICAgICAuZmlsdGVyKGl0ZW0gPT4gaXRlbSAhPT0gY29sbGFwc2UpXG4gICAgICAgIC5mb3JFYWNoKGl0ZW0gPT4ge1xuICAgICAgICAgIGlmIChpdGVtLm56QWN0aXZlKSB7XG4gICAgICAgICAgICBpdGVtLm56QWN0aXZlID0gZmFsc2U7XG4gICAgICAgICAgICBpdGVtLm56QWN0aXZlQ2hhbmdlLmVtaXQoaXRlbS5uekFjdGl2ZSk7XG4gICAgICAgICAgICBpdGVtLm1hcmtGb3JDaGVjaygpO1xuICAgICAgICAgIH1cbiAgICAgICAgfSk7XG4gICAgfVxuICAgIGNvbGxhcHNlLm56QWN0aXZlID0gIWNvbGxhcHNlLm56QWN0aXZlO1xuICAgIGNvbGxhcHNlLm56QWN0aXZlQ2hhbmdlLmVtaXQoY29sbGFwc2UubnpBY3RpdmUpO1xuICB9XG59XG4iXX0=