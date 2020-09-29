/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { OverlayModule } from '@angular/cdk/overlay';
import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { NzNoAnimationModule } from '../core/no-animation/nz-no-animation.module';
import { NzButtonModule } from '../button/nz-button.module';
import { NzIconModule } from '../icon/nz-icon.module';
import { NzMenuDividerDirective } from '../menu/nz-menu-divider.directive';
import { NzMenuGroupComponent } from '../menu/nz-menu-group.component';
import { NzMenuItemDirective } from '../menu/nz-menu-item.directive';
import { NzMenuDirective } from '../menu/nz-menu.directive';
import { NzSubMenuComponent } from '../menu/nz-submenu.component';
var NzMenuModule = /** @class */ (function () {
    function NzMenuModule() {
    }
    NzMenuModule.decorators = [
        { type: NgModule, args: [{
                    imports: [CommonModule, FormsModule, NzButtonModule, OverlayModule, NzIconModule, NzNoAnimationModule],
                    declarations: [
                        NzMenuDirective,
                        NzMenuItemDirective,
                        NzSubMenuComponent,
                        NzMenuDividerDirective,
                        NzMenuGroupComponent
                    ],
                    exports: [NzMenuDirective, NzMenuItemDirective, NzSubMenuComponent, NzMenuDividerDirective, NzMenuGroupComponent]
                },] }
    ];
    return NzMenuModule;
}());
export { NzMenuModule };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotbWVudS5tb2R1bGUuanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsibWVudS9uei1tZW51Lm1vZHVsZS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7O0FBQUEsT0FBTyxFQUFFLGFBQWEsRUFBRSxNQUFNLHNCQUFzQixDQUFDO0FBQ3JELE9BQU8sRUFBRSxZQUFZLEVBQUUsTUFBTSxpQkFBaUIsQ0FBQztBQUMvQyxPQUFPLEVBQUUsUUFBUSxFQUFFLE1BQU0sZUFBZSxDQUFDO0FBQ3pDLE9BQU8sRUFBRSxXQUFXLEVBQUUsTUFBTSxnQkFBZ0IsQ0FBQztBQUU3QyxPQUFPLEVBQUUsbUJBQW1CLEVBQUUsTUFBTSw2Q0FBNkMsQ0FBQztBQUVsRixPQUFPLEVBQUUsY0FBYyxFQUFFLE1BQU0sNEJBQTRCLENBQUM7QUFDNUQsT0FBTyxFQUFFLFlBQVksRUFBRSxNQUFNLHdCQUF3QixDQUFDO0FBQ3RELE9BQU8sRUFBRSxzQkFBc0IsRUFBRSxNQUFNLG1DQUFtQyxDQUFDO0FBQzNFLE9BQU8sRUFBRSxvQkFBb0IsRUFBRSxNQUFNLGlDQUFpQyxDQUFDO0FBQ3ZFLE9BQU8sRUFBRSxtQkFBbUIsRUFBRSxNQUFNLGdDQUFnQyxDQUFDO0FBQ3JFLE9BQU8sRUFBRSxlQUFlLEVBQUUsTUFBTSwyQkFBMkIsQ0FBQztBQUM1RCxPQUFPLEVBQUUsa0JBQWtCLEVBQUUsTUFBTSw4QkFBOEIsQ0FBQztBQUVsRTtJQUFBO0lBVzJCLENBQUM7O2dCQVgzQixRQUFRLFNBQUM7b0JBQ1IsT0FBTyxFQUFFLENBQUMsWUFBWSxFQUFFLFdBQVcsRUFBRSxjQUFjLEVBQUUsYUFBYSxFQUFFLFlBQVksRUFBRSxtQkFBbUIsQ0FBQztvQkFDdEcsWUFBWSxFQUFFO3dCQUNaLGVBQWU7d0JBQ2YsbUJBQW1CO3dCQUNuQixrQkFBa0I7d0JBQ2xCLHNCQUFzQjt3QkFDdEIsb0JBQW9CO3FCQUNyQjtvQkFDRCxPQUFPLEVBQUUsQ0FBQyxlQUFlLEVBQUUsbUJBQW1CLEVBQUUsa0JBQWtCLEVBQUUsc0JBQXNCLEVBQUUsb0JBQW9CLENBQUM7aUJBQ2xIOztJQUMwQixtQkFBQztDQUFBLEFBWDVCLElBVzRCO1NBQWYsWUFBWSIsInNvdXJjZXNDb250ZW50IjpbImltcG9ydCB7IE92ZXJsYXlNb2R1bGUgfSBmcm9tICdAYW5ndWxhci9jZGsvb3ZlcmxheSc7XG5pbXBvcnQgeyBDb21tb25Nb2R1bGUgfSBmcm9tICdAYW5ndWxhci9jb21tb24nO1xuaW1wb3J0IHsgTmdNb2R1bGUgfSBmcm9tICdAYW5ndWxhci9jb3JlJztcbmltcG9ydCB7IEZvcm1zTW9kdWxlIH0gZnJvbSAnQGFuZ3VsYXIvZm9ybXMnO1xuXG5pbXBvcnQgeyBOek5vQW5pbWF0aW9uTW9kdWxlIH0gZnJvbSAnLi4vY29yZS9uby1hbmltYXRpb24vbnotbm8tYW5pbWF0aW9uLm1vZHVsZSc7XG5cbmltcG9ydCB7IE56QnV0dG9uTW9kdWxlIH0gZnJvbSAnLi4vYnV0dG9uL256LWJ1dHRvbi5tb2R1bGUnO1xuaW1wb3J0IHsgTnpJY29uTW9kdWxlIH0gZnJvbSAnLi4vaWNvbi9uei1pY29uLm1vZHVsZSc7XG5pbXBvcnQgeyBOek1lbnVEaXZpZGVyRGlyZWN0aXZlIH0gZnJvbSAnLi4vbWVudS9uei1tZW51LWRpdmlkZXIuZGlyZWN0aXZlJztcbmltcG9ydCB7IE56TWVudUdyb3VwQ29tcG9uZW50IH0gZnJvbSAnLi4vbWVudS9uei1tZW51LWdyb3VwLmNvbXBvbmVudCc7XG5pbXBvcnQgeyBOek1lbnVJdGVtRGlyZWN0aXZlIH0gZnJvbSAnLi4vbWVudS9uei1tZW51LWl0ZW0uZGlyZWN0aXZlJztcbmltcG9ydCB7IE56TWVudURpcmVjdGl2ZSB9IGZyb20gJy4uL21lbnUvbnotbWVudS5kaXJlY3RpdmUnO1xuaW1wb3J0IHsgTnpTdWJNZW51Q29tcG9uZW50IH0gZnJvbSAnLi4vbWVudS9uei1zdWJtZW51LmNvbXBvbmVudCc7XG5cbkBOZ01vZHVsZSh7XG4gIGltcG9ydHM6IFtDb21tb25Nb2R1bGUsIEZvcm1zTW9kdWxlLCBOekJ1dHRvbk1vZHVsZSwgT3ZlcmxheU1vZHVsZSwgTnpJY29uTW9kdWxlLCBOek5vQW5pbWF0aW9uTW9kdWxlXSxcbiAgZGVjbGFyYXRpb25zOiBbXG4gICAgTnpNZW51RGlyZWN0aXZlLFxuICAgIE56TWVudUl0ZW1EaXJlY3RpdmUsXG4gICAgTnpTdWJNZW51Q29tcG9uZW50LFxuICAgIE56TWVudURpdmlkZXJEaXJlY3RpdmUsXG4gICAgTnpNZW51R3JvdXBDb21wb25lbnRcbiAgXSxcbiAgZXhwb3J0czogW056TWVudURpcmVjdGl2ZSwgTnpNZW51SXRlbURpcmVjdGl2ZSwgTnpTdWJNZW51Q29tcG9uZW50LCBOek1lbnVEaXZpZGVyRGlyZWN0aXZlLCBOek1lbnVHcm91cENvbXBvbmVudF1cbn0pXG5leHBvcnQgY2xhc3MgTnpNZW51TW9kdWxlIHt9XG4iXX0=