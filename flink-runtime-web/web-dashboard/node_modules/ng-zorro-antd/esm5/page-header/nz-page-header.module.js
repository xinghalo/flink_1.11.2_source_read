/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import * as tslib_1 from "tslib";
import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { NzAddOnModule } from '../core/addon/addon.module';
import { NzDividerModule } from '../divider/nz-divider.module';
import { NzIconModule } from '../icon/nz-icon.module';
import { NzPageHeaderBreadcrumbDirective, NzPageHeaderContentDirective, NzPageHeaderExtraDirective, NzPageHeaderFooterDirective, NzPageHeaderSubtitleDirective, NzPageHeaderTagDirective, NzPageHeaderTitleDirective } from './nz-page-header-cells';
import { NzPageHeaderComponent } from './nz-page-header.component';
/** @type {?} */
var NzPageHeaderCells = [
    NzPageHeaderTitleDirective,
    NzPageHeaderSubtitleDirective,
    NzPageHeaderContentDirective,
    NzPageHeaderTagDirective,
    NzPageHeaderExtraDirective,
    NzPageHeaderFooterDirective,
    NzPageHeaderBreadcrumbDirective
];
var NzPageHeaderModule = /** @class */ (function () {
    function NzPageHeaderModule() {
    }
    NzPageHeaderModule.decorators = [
        { type: NgModule, args: [{
                    imports: [CommonModule, NzAddOnModule, NzIconModule, NzDividerModule],
                    exports: tslib_1.__spread([NzPageHeaderComponent], NzPageHeaderCells),
                    declarations: tslib_1.__spread([NzPageHeaderComponent], NzPageHeaderCells)
                },] }
    ];
    return NzPageHeaderModule;
}());
export { NzPageHeaderModule };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotcGFnZS1oZWFkZXIubW9kdWxlLmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbInBhZ2UtaGVhZGVyL256LXBhZ2UtaGVhZGVyLm1vZHVsZS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7OztBQUFBLE9BQU8sRUFBRSxZQUFZLEVBQUUsTUFBTSxpQkFBaUIsQ0FBQztBQUMvQyxPQUFPLEVBQUUsUUFBUSxFQUFFLE1BQU0sZUFBZSxDQUFDO0FBQ3pDLE9BQU8sRUFBRSxhQUFhLEVBQUUsTUFBTSw0QkFBNEIsQ0FBQztBQUMzRCxPQUFPLEVBQUUsZUFBZSxFQUFFLE1BQU0sOEJBQThCLENBQUM7QUFDL0QsT0FBTyxFQUFFLFlBQVksRUFBRSxNQUFNLHdCQUF3QixDQUFDO0FBQ3RELE9BQU8sRUFDTCwrQkFBK0IsRUFDL0IsNEJBQTRCLEVBQUUsMEJBQTBCLEVBQUUsMkJBQTJCLEVBQ3JGLDZCQUE2QixFQUFFLHdCQUF3QixFQUN2RCwwQkFBMEIsRUFDM0IsTUFBTSx3QkFBd0IsQ0FBQztBQUNoQyxPQUFPLEVBQUUscUJBQXFCLEVBQUUsTUFBTSw0QkFBNEIsQ0FBQzs7SUFFN0QsaUJBQWlCLEdBQUc7SUFDeEIsMEJBQTBCO0lBQzFCLDZCQUE2QjtJQUM3Qiw0QkFBNEI7SUFDNUIsd0JBQXdCO0lBQ3hCLDBCQUEwQjtJQUMxQiwyQkFBMkI7SUFDM0IsK0JBQStCO0NBQ2hDO0FBRUQ7SUFBQTtJQU1BLENBQUM7O2dCQU5BLFFBQVEsU0FBQztvQkFDUixPQUFPLEVBQU8sQ0FBRSxZQUFZLEVBQUUsYUFBYSxFQUFFLFlBQVksRUFBRSxlQUFlLENBQUU7b0JBQzVFLE9BQU8sb0JBQVMscUJBQXFCLEdBQUssaUJBQWlCLENBQUU7b0JBQzdELFlBQVksb0JBQUkscUJBQXFCLEdBQUssaUJBQWlCLENBQUU7aUJBQzlEOztJQUVELHlCQUFDO0NBQUEsQUFORCxJQU1DO1NBRFksa0JBQWtCIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHsgQ29tbW9uTW9kdWxlIH0gZnJvbSAnQGFuZ3VsYXIvY29tbW9uJztcbmltcG9ydCB7IE5nTW9kdWxlIH0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5pbXBvcnQgeyBOekFkZE9uTW9kdWxlIH0gZnJvbSAnLi4vY29yZS9hZGRvbi9hZGRvbi5tb2R1bGUnO1xuaW1wb3J0IHsgTnpEaXZpZGVyTW9kdWxlIH0gZnJvbSAnLi4vZGl2aWRlci9uei1kaXZpZGVyLm1vZHVsZSc7XG5pbXBvcnQgeyBOekljb25Nb2R1bGUgfSBmcm9tICcuLi9pY29uL256LWljb24ubW9kdWxlJztcbmltcG9ydCB7XG4gIE56UGFnZUhlYWRlckJyZWFkY3J1bWJEaXJlY3RpdmUsXG4gIE56UGFnZUhlYWRlckNvbnRlbnREaXJlY3RpdmUsIE56UGFnZUhlYWRlckV4dHJhRGlyZWN0aXZlLCBOelBhZ2VIZWFkZXJGb290ZXJEaXJlY3RpdmUsXG4gIE56UGFnZUhlYWRlclN1YnRpdGxlRGlyZWN0aXZlLCBOelBhZ2VIZWFkZXJUYWdEaXJlY3RpdmUsXG4gIE56UGFnZUhlYWRlclRpdGxlRGlyZWN0aXZlXG59IGZyb20gJy4vbnotcGFnZS1oZWFkZXItY2VsbHMnO1xuaW1wb3J0IHsgTnpQYWdlSGVhZGVyQ29tcG9uZW50IH0gZnJvbSAnLi9uei1wYWdlLWhlYWRlci5jb21wb25lbnQnO1xuXG5jb25zdCBOelBhZ2VIZWFkZXJDZWxscyA9IFtcbiAgTnpQYWdlSGVhZGVyVGl0bGVEaXJlY3RpdmUsXG4gIE56UGFnZUhlYWRlclN1YnRpdGxlRGlyZWN0aXZlLFxuICBOelBhZ2VIZWFkZXJDb250ZW50RGlyZWN0aXZlLFxuICBOelBhZ2VIZWFkZXJUYWdEaXJlY3RpdmUsXG4gIE56UGFnZUhlYWRlckV4dHJhRGlyZWN0aXZlLFxuICBOelBhZ2VIZWFkZXJGb290ZXJEaXJlY3RpdmUsXG4gIE56UGFnZUhlYWRlckJyZWFkY3J1bWJEaXJlY3RpdmVcbl07XG5cbkBOZ01vZHVsZSh7XG4gIGltcG9ydHMgICAgIDogWyBDb21tb25Nb2R1bGUsIE56QWRkT25Nb2R1bGUsIE56SWNvbk1vZHVsZSwgTnpEaXZpZGVyTW9kdWxlIF0sXG4gIGV4cG9ydHMgICAgIDogWyBOelBhZ2VIZWFkZXJDb21wb25lbnQsIC4uLk56UGFnZUhlYWRlckNlbGxzIF0sXG4gIGRlY2xhcmF0aW9uczogWyBOelBhZ2VIZWFkZXJDb21wb25lbnQsIC4uLk56UGFnZUhlYWRlckNlbGxzIF1cbn0pXG5leHBvcnQgY2xhc3MgTnpQYWdlSGVhZGVyTW9kdWxlIHtcbn1cbiJdfQ==