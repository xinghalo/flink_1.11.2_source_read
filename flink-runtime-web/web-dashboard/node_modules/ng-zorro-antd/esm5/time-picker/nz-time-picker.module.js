/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { OverlayModule } from '@angular/cdk/overlay';
import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { NzOverlayModule } from '../core/overlay/nz-overlay.module';
import { NzI18nModule } from '../i18n/nz-i18n.module';
import { NzIconModule } from '../icon/nz-icon.module';
import { NzTimePickerPanelComponent } from './nz-time-picker-panel.component';
import { NzTimePickerComponent } from './nz-time-picker.component';
import { NzTimeValueAccessorDirective } from './nz-time-value-accessor.directive';
var NzTimePickerModule = /** @class */ (function () {
    function NzTimePickerModule() {
    }
    NzTimePickerModule.decorators = [
        { type: NgModule, args: [{
                    declarations: [NzTimePickerComponent, NzTimePickerPanelComponent, NzTimeValueAccessorDirective],
                    exports: [NzTimePickerPanelComponent, NzTimePickerComponent],
                    imports: [CommonModule, FormsModule, NzI18nModule, OverlayModule, NzIconModule, NzOverlayModule],
                    entryComponents: []
                },] }
    ];
    return NzTimePickerModule;
}());
export { NzTimePickerModule };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotdGltZS1waWNrZXIubW9kdWxlLmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbInRpbWUtcGlja2VyL256LXRpbWUtcGlja2VyLm1vZHVsZS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7O0FBQUEsT0FBTyxFQUFFLGFBQWEsRUFBRSxNQUFNLHNCQUFzQixDQUFDO0FBQ3JELE9BQU8sRUFBRSxZQUFZLEVBQUUsTUFBTSxpQkFBaUIsQ0FBQztBQUMvQyxPQUFPLEVBQUUsUUFBUSxFQUFFLE1BQU0sZUFBZSxDQUFDO0FBQ3pDLE9BQU8sRUFBRSxXQUFXLEVBQUUsTUFBTSxnQkFBZ0IsQ0FBQztBQUM3QyxPQUFPLEVBQUUsZUFBZSxFQUFFLE1BQU0sbUNBQW1DLENBQUM7QUFDcEUsT0FBTyxFQUFFLFlBQVksRUFBRSxNQUFNLHdCQUF3QixDQUFDO0FBQ3RELE9BQU8sRUFBRSxZQUFZLEVBQUUsTUFBTSx3QkFBd0IsQ0FBQztBQUN0RCxPQUFPLEVBQUUsMEJBQTBCLEVBQUUsTUFBTSxrQ0FBa0MsQ0FBQztBQUM5RSxPQUFPLEVBQUUscUJBQXFCLEVBQUUsTUFBTSw0QkFBNEIsQ0FBQztBQUNuRSxPQUFPLEVBQUUsNEJBQTRCLEVBQUUsTUFBTSxvQ0FBb0MsQ0FBQztBQUVsRjtJQUFBO0lBTWlDLENBQUM7O2dCQU5qQyxRQUFRLFNBQUM7b0JBQ1IsWUFBWSxFQUFFLENBQUMscUJBQXFCLEVBQUUsMEJBQTBCLEVBQUUsNEJBQTRCLENBQUM7b0JBQy9GLE9BQU8sRUFBRSxDQUFDLDBCQUEwQixFQUFFLHFCQUFxQixDQUFDO29CQUM1RCxPQUFPLEVBQUUsQ0FBQyxZQUFZLEVBQUUsV0FBVyxFQUFFLFlBQVksRUFBRSxhQUFhLEVBQUUsWUFBWSxFQUFFLGVBQWUsQ0FBQztvQkFDaEcsZUFBZSxFQUFFLEVBQUU7aUJBQ3BCOztJQUNnQyx5QkFBQztDQUFBLEFBTmxDLElBTWtDO1NBQXJCLGtCQUFrQiIsInNvdXJjZXNDb250ZW50IjpbImltcG9ydCB7IE92ZXJsYXlNb2R1bGUgfSBmcm9tICdAYW5ndWxhci9jZGsvb3ZlcmxheSc7XG5pbXBvcnQgeyBDb21tb25Nb2R1bGUgfSBmcm9tICdAYW5ndWxhci9jb21tb24nO1xuaW1wb3J0IHsgTmdNb2R1bGUgfSBmcm9tICdAYW5ndWxhci9jb3JlJztcbmltcG9ydCB7IEZvcm1zTW9kdWxlIH0gZnJvbSAnQGFuZ3VsYXIvZm9ybXMnO1xuaW1wb3J0IHsgTnpPdmVybGF5TW9kdWxlIH0gZnJvbSAnLi4vY29yZS9vdmVybGF5L256LW92ZXJsYXkubW9kdWxlJztcbmltcG9ydCB7IE56STE4bk1vZHVsZSB9IGZyb20gJy4uL2kxOG4vbnotaTE4bi5tb2R1bGUnO1xuaW1wb3J0IHsgTnpJY29uTW9kdWxlIH0gZnJvbSAnLi4vaWNvbi9uei1pY29uLm1vZHVsZSc7XG5pbXBvcnQgeyBOelRpbWVQaWNrZXJQYW5lbENvbXBvbmVudCB9IGZyb20gJy4vbnotdGltZS1waWNrZXItcGFuZWwuY29tcG9uZW50JztcbmltcG9ydCB7IE56VGltZVBpY2tlckNvbXBvbmVudCB9IGZyb20gJy4vbnotdGltZS1waWNrZXIuY29tcG9uZW50JztcbmltcG9ydCB7IE56VGltZVZhbHVlQWNjZXNzb3JEaXJlY3RpdmUgfSBmcm9tICcuL256LXRpbWUtdmFsdWUtYWNjZXNzb3IuZGlyZWN0aXZlJztcblxuQE5nTW9kdWxlKHtcbiAgZGVjbGFyYXRpb25zOiBbTnpUaW1lUGlja2VyQ29tcG9uZW50LCBOelRpbWVQaWNrZXJQYW5lbENvbXBvbmVudCwgTnpUaW1lVmFsdWVBY2Nlc3NvckRpcmVjdGl2ZV0sXG4gIGV4cG9ydHM6IFtOelRpbWVQaWNrZXJQYW5lbENvbXBvbmVudCwgTnpUaW1lUGlja2VyQ29tcG9uZW50XSxcbiAgaW1wb3J0czogW0NvbW1vbk1vZHVsZSwgRm9ybXNNb2R1bGUsIE56STE4bk1vZHVsZSwgT3ZlcmxheU1vZHVsZSwgTnpJY29uTW9kdWxlLCBOek92ZXJsYXlNb2R1bGVdLFxuICBlbnRyeUNvbXBvbmVudHM6IFtdXG59KVxuZXhwb3J0IGNsYXNzIE56VGltZVBpY2tlck1vZHVsZSB7fVxuIl19